package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Represents a claim on resource (represented by an int) from a finite pool of resources negotiated through a
 * queueing protocol facilitated by a ZooKeeper-quorum.
 */
public class ResourceClaim implements ZooKeeperConnectionObserver {
    final static Logger logger = LoggerFactory.getLogger(ResourceClaim.class);

    final static String QUEUE_NODE = "/unique-id-generator/queue";
    final static String POOL_NODE = "/unique-id-generator/pool";
    final static String LOCKING_TICKET = "nr-00000000000000";

    static final Random random = new Random();

    final int resource;
    final int poolSize;
    final ZooKeeper zookeeper;
    protected State state = State.UNCLAIMED;

    ResourceClaim(ZooKeeper zookeeper, int poolSize) throws IOException {
        ZooKeeperConnection.registerObserver(this);
        this.poolSize = poolSize;
        this.zookeeper = zookeeper;
        if (zookeeper.getState() != ZooKeeper.States.CONNECTED) {
            throw new IOException("Not connected to ZooKeeper quorum");
        }
        try {
            String placeInLine = acquireLock(zookeeper, QUEUE_NODE);
            this.resource = claimResource(zookeeper, POOL_NODE, poolSize);
            releaseTicket(zookeeper, QUEUE_NODE, placeInLine);
        } catch (KeeperException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        state = State.HAS_CLAIM;
    }

    /**
     * Claim a resource.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param poolSize Size of the resource pool.
     * @return A resource claim.
     * @throws IOException
     */
    public static ResourceClaim claim(ZooKeeper zookeeper, int poolSize) throws IOException {
        return new ResourceClaim(zookeeper, poolSize);
    }

    /**
     * Get the claimed resource.
     *
     * @return The resource claimed.
     * @throws java.lang.IllegalStateException Thrown when the claim is no longer held.
     */
    public int get() {
        if (state != State.HAS_CLAIM) {
            throw new IllegalStateException("Resource claim not held.");
        }
        return resource;
    }

    /**
     * Relinquish the claim to this resource, and release it back to the resource pool.
     */
    public void close() {
        if (state == State.CLAIM_RELINQUISHED) {
            // Already relinquished, nothing to do.
            return;
        }
        state = State.CLAIM_RELINQUISHED;
        relinquishResource(zookeeper, POOL_NODE, resource);
        ZooKeeperConnection.unregisterObserver(this);
    }

    /**
     * Try to acquire a lock on for choosing a resource. This method will wait until it has acquired the lock.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param lockNode Path to the znode representing the locking queue.
     * @return Name of the first node in the queue.
     * @throws KeeperException
     * @throws InterruptedException
     */
    static String acquireLock(ZooKeeper zookeeper, String lockNode) throws KeeperException, InterruptedException {
        // Inspired by the queueing algorithm suggested here:
        // http://zookeeper.apache.org/doc/trunk/recipes.html#sc_recipes_Queues

        // Acquire a place in the queue by creating an ephemeral, sequential znode.
        String placeInLine = takeQueueTicket(zookeeper, lockNode);
        logger.debug("Acquiring lock, waiting in queue: {}.", placeInLine);

        // Wait in the queue until our turn has come.
        return waitInLine(zookeeper, lockNode, placeInLine);
    }

    /**
     * Take a ticket for the queue. If the ticket was already claimed by another process,
     * this method retires until it succeeds.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param lockNode Path to the znode representing the locking queue.
     * @return The claimed ticket.
     * @throws InterruptedException
     * @throws KeeperException
     */
    static String takeQueueTicket(ZooKeeper zookeeper, String lockNode) throws InterruptedException, KeeperException {
        // The ticket number includes a random component to decrease the chances of collision. Collision is handled
        // neatly, but it saves a few actions if there is no need to retry ticket acquisition.
        String ticket = String.format("nr-%014d-%04d", System.currentTimeMillis(), random.nextInt(10000));
        if (grabTicket(zookeeper, lockNode, ticket)) {
            return ticket;
        } else {
            return takeQueueTicket(zookeeper, lockNode);
        }
    }

    /**
     * Release an acquired lock.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param lockNode Path to the znode representing the locking queue.
     * @param ticket Name of the first node in the queue.
     * @throws KeeperException
     * @throws InterruptedException
     */
    static void releaseTicket(ZooKeeper zookeeper, String lockNode, String ticket)
            throws KeeperException, InterruptedException {

        logger.debug("Releasing ticket {}.", ticket);
        try {
            zookeeper.delete(lockNode + "/" + ticket, -1);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NONODE) {
                // If it the node is already gone, than that is fine, otherwise:
                throw e;
            }
        }
    }

    /**
     * Wait in the queue until the znode in front of us changes.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param lockNode Path to the znode representing the locking queue.
     * @param placeInLine Name of our current position in the queue.
     * @return Name of the first node in the queue, when we are it.
     * @throws KeeperException
     * @throws InterruptedException
     */
    static String waitInLine(ZooKeeper zookeeper, String lockNode, String placeInLine)
            throws KeeperException, InterruptedException {

        // Get the list of nodes in the queue, and find out what our position is.
        List<String> children = zookeeper.getChildren(lockNode, false);

        // The list returned is unsorted.
        Collections.sort(children);

        if (children.size() == 0) {
            // Only possible if some other process cancelled our ticket.
            logger.warn("getChildren() returned empty list, but we created a ticket.");
            return acquireLock(zookeeper, lockNode);
        }

        boolean lockingTicketExists = children.get(0).equals(LOCKING_TICKET);
        if (lockingTicketExists) {
            children.remove(0);
        }

        // Where are we in the queue?
        int positionInQueue = -1;
        int i = 0;
        for (String child : children) {
            if (child.equals(placeInLine)) {
                positionInQueue = i;
                break;
            }
            i++;
        }

        if (positionInQueue < 0) {
            // Theoretically not possible.
            throw new RuntimeException("Created node (" + placeInLine + ") not found in getChildren().");
        }

        String placeBeforeUs;
        if (positionInQueue == 0) {
            // Lowest number in the queue, go for the lock.
            if (grabTicket(zookeeper, lockNode, LOCKING_TICKET)) {
                releaseTicket(zookeeper, lockNode, placeInLine);
                return LOCKING_TICKET;
            } else {
                placeBeforeUs = LOCKING_TICKET;
            }
        } else {
            // We are not in front of the queue, so we keep an eye on the znode right in front of us. When it is
            // deleted, that means it has reached the front of the queue, acquired the lock, did its business,
            // and released the lock.
            placeBeforeUs = children.get(positionInQueue - 1);
        }

        final CountDownLatch latch = new CountDownLatch(1);
        Stat stat = zookeeper.exists(lockNode + "/" + placeBeforeUs, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // If *anything* changes, reevaluate out position in the queue.
                latch.countDown();
            }
        });

        // If stat is null, the znode in front of use got deleted during our inspection of the queue. If that happens,
        // simply reevaluate our position in the queue again. If there *is* a znode in front of us,
        // watch it for changes:
        if (stat != null) {
            logger.debug("Watching place in queue before us ({})", placeBeforeUs);
            latch.await();
        }

        return waitInLine(zookeeper, lockNode, placeInLine);
    }

    /**
     * Grab a ticket in the queue.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param lockNode Path to the znode representing the locking queue.
     * @param ticket Name of the ticket to attempt to grab.
     * @return True on success, false if the ticket was already grabbed by another process.
     * @throws InterruptedException
     * @throws KeeperException
     */
    static boolean grabTicket(ZooKeeper zookeeper, String lockNode, String ticket)
            throws InterruptedException, KeeperException {
        try {
            zookeeper.create(lockNode + "/" + ticket, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (KeeperException e) {
            if (e.code() == KeeperException.Code.NODEEXISTS) {
                // It is possible that two processes try to grab the exact same ticket at the same time.
                // This is common for the locking ticket.
                logger.debug("Failed to claim ticket {}.", ticket);
                return false;
            } else {
                throw e;
            }
        }
        logger.debug("Claimed ticket {}.", ticket);
        return true;
    }

    /**
     * Try to claim an available resource from the resource pool.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param poolNode Path to the znode representing the resource pool.
     * @param poolSize Size of the resource pool.
     * @return The claimed resource.
     * @throws KeeperException
     * @throws InterruptedException
     */
    static int claimResource(ZooKeeper zookeeper, String poolNode, int poolSize)
            throws KeeperException, InterruptedException {

        logger.debug("Trying to claim a resource.");
        List<String> claimedResources = zookeeper.getChildren(poolNode, false);
        if (claimedResources.size() >= poolSize) {
            logger.debug("No resources available at the moment (poolsize: {}), waiting.", poolSize);
            // No resources available. Wait for a resource to become available.
            final CountDownLatch latch = new CountDownLatch(1);
            zookeeper.getChildren(poolNode, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    latch.countDown();
                }
            });
            latch.await();
            return claimResource(zookeeper, poolNode, poolSize);
        }

        // Try to claim an available resource.
        for (int i = 0; i < poolSize; i++) {
            String resourcePath = Integer.toString(i);
            if (!claimedResources.contains(resourcePath)) {
                try {
                    logger.debug("Trying to claim seemingly available resource {}.", resourcePath);
                    zookeeper.create(poolNode + "/" + resourcePath, new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (KeeperException e) {
                    if (e.code() == KeeperException.Code.NODEEXISTS) {
                        // Failed to claim this resource for some reason.
                        continue;
                    } else {
                        // Unexpected failure.
                        throw e;
                    }
                }
                logger.debug("Successfully claimed resource {}.", resourcePath);
                return i;
            }
        }

        return claimResource(zookeeper, poolNode, poolSize);
    }

    /**
     * Relinquish a claimed resource.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param poolNode Path to the znode representing the resource pool.
     * @param resource The resource.
     */
    private void relinquishResource(ZooKeeper zookeeper, String poolNode, int resource) {
        logger.debug("Relinquishing claimed resource {}.", resource);
        try {
            zookeeper.delete(poolNode + "/" + Integer.toString(resource), -1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            logger.error("Failed to remove resource claim node {}/{}", poolNode, resource);
        }
    }

    @Override
    public void disconnected() {
        logger.debug("Disconnected from ZooKeeper quorum, this invalidates the claim to resource {}.", resource);
        state = State.CLAIM_RELINQUISHED;
        ZooKeeperConnection.unregisterObserver(this);
    }

    @Override
    public void connected() {
        // NOOP.
    }

    /**
     * Internal state of this ResourceClaim.
     */
    public enum State {
        UNCLAIMED,
        HAS_CLAIM,
        CLAIM_RELINQUISHED
    }
}

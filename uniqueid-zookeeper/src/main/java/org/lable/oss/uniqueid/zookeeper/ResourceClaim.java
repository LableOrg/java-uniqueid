/*
 * Copyright (C) 2014 Lable (info@lable.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lable.oss.uniqueid.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnection;
import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnectionObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.lable.oss.uniqueid.zookeeper.ZooKeeperHelper.createIfNotThere;
import static org.lable.oss.uniqueid.zookeeper.ZooKeeperHelper.mkdirp;

/**
 * Represents a claim on resource (represented by an int) from a finite pool of resources negotiated through a
 * queueing protocol facilitated by a ZooKeeper-quorum.
 */
public class ResourceClaim implements ZooKeeperConnectionObserver, Closeable {
    final static Logger logger = LoggerFactory.getLogger(ResourceClaim.class);

    static String ZNODE;
    static String QUEUE_NODE;
    static String POOL_NODE;
    final static String LOCKING_TICKET = "nr-00000000000000";

    static final Random random = new Random();
    final int resource;

    final int poolSize;
    final ZooKeeper zookeeper;
    final ZooKeeperConnection zooKeeperConnection;

    protected State state = State.UNCLAIMED;

    ResourceClaim(ZooKeeperConnection zooKeeperConnection, int poolSize, String znode) throws IOException {
        logger.debug("Acquiring resource-claim…");

        ZNODE = znode;
        QUEUE_NODE = znode + "/queue";
        POOL_NODE = znode + "/pool";
        zooKeeperConnection.registerObserver(this);
        this.poolSize = poolSize;
        this.zooKeeperConnection = zooKeeperConnection;
        this.zookeeper = zooKeeperConnection.get();

        if (zookeeper.getState() != ZooKeeper.States.CONNECTED) {
            throw new IOException("Not connected to ZooKeeper quorum.");
        }

        try {
            ensureRequiredZnodesExist(zookeeper, znode);
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

        logger.debug("Resource-claim acquired ({}).", resource);
    }

    /**
     * Make sure the required znodes are present on the quorum.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param znode Base-path for our znodes.
     */
    void ensureRequiredZnodesExist(ZooKeeper zookeeper, String znode) throws KeeperException, InterruptedException {
        mkdirp(zookeeper, znode);
        createIfNotThere(zookeeper, QUEUE_NODE);
        createIfNotThere(zookeeper, POOL_NODE);
    }

    /**
     * Claim a resource.
     *
     * @param zooKeeperConnection ZooKeeper connection to use.
     * @param poolSize            Size of the resource pool.
     * @param znode               Root znode of the ZooKeeper resource-pool.
     * @return A resource claim.
     */
    public static ResourceClaim claim(ZooKeeperConnection zooKeeperConnection, int poolSize, String znode) throws IOException {
        return new ResourceClaim(zooKeeperConnection, poolSize, znode);
    }

    /**
     * Get the claimed resource.
     *
     * @return The resource claimed.
     * @throws IllegalStateException Thrown when the claim is no longer held.
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
        close(false);
    }

    public void close(boolean nodeAlreadyDeleted) {
        if (state == State.CLAIM_RELINQUISHED) {
            // Already relinquished, nothing to do.
            return;
        }
        state = State.CLAIM_RELINQUISHED;
        zooKeeperConnection.deregisterObserver(this);

        logger.debug("Closing resource-claim ({}).", resource);

        // No need to delete the node if the reason we are closing is the deletion of said node.
        if (nodeAlreadyDeleted) return;

        // Hang on to the claimed resource without using it for a short while to facilitate clock skew.
        // That is, if any participant is generating IDs with a slightly skewed clock, it can generate IDs that
        // overlap with the ones generated by the participant who successfully claims the same resource before or
        // after. By hanging on to each resource for a bit a slight clock skew may be handled gracefully.
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                relinquishResource(zookeeper, POOL_NODE, resource);
            }
            // Two seconds seems reasonable. The NTP docs state that clocks running more than 128ms out of sync are
            // rare under normal conditions.
        }, TimeUnit.SECONDS.toMillis(2));
    }

    /**
     * Try to acquire a lock on for choosing a resource. This method will wait until it has acquired the lock.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param lockNode Path to the znode representing the locking queue.
     * @return Name of the first node in the queue.
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
     * this method retries until it succeeds.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param lockNode Path to the znode representing the locking queue.
     * @return The claimed ticket.
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
        Stat stat = zookeeper.exists(lockNode + "/" + placeBeforeUs, event -> {
            // If *anything* changes, reevaluate our position in the queue.
            latch.countDown();
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
     */
    int claimResource(ZooKeeper zookeeper, String poolNode, int poolSize)
            throws KeeperException, InterruptedException {

        logger.debug("Trying to claim a resource.");
        List<String> claimedResources = zookeeper.getChildren(poolNode, false);
        if (claimedResources.size() >= poolSize) {
            logger.debug("No resources available at the moment (pool size: {}), waiting.", poolSize);
            // No resources available. Wait for a resource to become available.
            final CountDownLatch latch = new CountDownLatch(1);
            zookeeper.getChildren(poolNode, event -> latch.countDown());
            latch.await();
            return claimResource(zookeeper, poolNode, poolSize);
        }

        // Try to claim an available resource.
        for (int i = 0; i < poolSize; i++) {
            String resourcePath = Integer.toString(i);
            if (!claimedResources.contains(resourcePath)) {
                String node;
                try {
                    logger.debug("Trying to claim seemingly available resource {}.", resourcePath);
                    node = zookeeper.create(
                            poolNode + "/" + resourcePath,
                            new byte[0],
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.EPHEMERAL
                    );
                } catch (KeeperException e) {
                    if (e.code() == KeeperException.Code.NODEEXISTS) {
                        // Failed to claim this resource for some reason.
                        continue;
                    } else {
                        // Unexpected failure.
                        throw e;
                    }
                }

                zookeeper.exists(node, event -> {
                    if (event.getType() == EventType.NodeDeleted) {
                        // Invalidate our claim when the node is deleted by some other process.
                        logger.debug("Resource-claim node unexpectedly deleted ({})", resource);
                        close(true);
                    }
                });

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

    public String getConfiguredZNode() {
        return ZNODE;
    }

    @Override
    public void disconnected() {
        logger.debug("Disconnected from ZooKeeper quorum, this invalidates the claim to resource {}.", resource);
        state = State.CLAIM_RELINQUISHED;
        zooKeeperConnection.deregisterObserver(this);
    }

    @Override
    public void connected() {
        // No-op.
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

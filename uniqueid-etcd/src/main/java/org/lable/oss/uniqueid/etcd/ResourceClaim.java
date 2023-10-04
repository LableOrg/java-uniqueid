/*
 * Copyright © 2014 Lable (info@lable.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lable.oss.uniqueid.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.OptionsUtil;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.support.CloseableClient;
import io.etcd.jetcd.watch.WatchEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Represents a claim on resource (represented by an int) from a finite pool of resources negotiated through a
 * queueing protocol facilitated by a ZooKeeper-quorum.
 */
public class ResourceClaim implements Closeable {
    static final String POOL_PREFIX = "pool/";
    static final ByteSequence POOL_KEY = ByteSequence.from(POOL_PREFIX, StandardCharsets.UTF_8);
    static final ByteSequence LOCK_NAME = ByteSequence.from("unique-id-resource-lock", StandardCharsets.UTF_8);

    final static Logger logger = LoggerFactory.getLogger(ResourceClaim.class);

    final int clusterId;
    final int generatorId;

    final int poolSize;
    final Client etcd;

    long leaseId;

    protected State state;

    protected List<Closeable> closeables = new ArrayList<>();

    ResourceClaim(Client etcd,
                  int maxGeneratorCount,
                  List<Integer> clusterIds,
                  Duration timeout) throws IOException {
        state = State.INITIALIZING;
        logger.debug("Acquiring resource-claim…");

        timeout = timeout == null ? Duration.ofMinutes(5) : timeout;
        Instant giveUpAfter = Instant.now().plus(timeout);

        this.poolSize = maxGeneratorCount;
        this.etcd = etcd;
        try {
            LeaseGrantResponse lease = etcd.getLeaseClient().grant(5).get();
            leaseId = lease.getID();

            // Keep the lease alive until we are done.
            CloseableClient leaseKeepAlive = EtcdHelper.keepLeaseAlive(etcd, leaseId, this::close);

            // Release the lease when closed.
            closeables.add(leaseKeepAlive::close);

            // Acquire the lock. This makes sure we are the only process claiming a resource.
            LockResponse lock;
            try {
                lock = etcd.getLockClient()
                        .lock(LOCK_NAME, leaseId)
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                close();
                throw new IOException("Process timed out.");
            }

            if (logger.isDebugEnabled()) {
                logger.debug("Acquired lock: {}.", lock.getKey().toString(StandardCharsets.UTF_8));
            }

            ResourcePair resourcePair = claimResource(etcd, maxGeneratorCount, clusterIds, giveUpAfter);
            this.clusterId = resourcePair.clusterId;
            this.generatorId = resourcePair.generatorId;

            // Release the lock. If this line is not reached due to exceptions raised, the lock will automatically
            // be removed when the lease holding it expires.
            etcd.getLockClient().unlock(lock.getKey()).get();
        } catch (ExecutionException e) {
            close();
            throw new IOException(e);
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        state = State.HAS_CLAIM;

        logger.debug("Resource-claim acquired ({}/{}).", clusterId, generatorId);
    }

    /**
     * Claim a resource.
     *
     * @param etcd              Etcd connection.
     * @param maxGeneratorCount Maximum number of generators possible.
     * @return A resource claim.
     */
    public static ResourceClaim claim(Client etcd,
                                      int maxGeneratorCount,
                                      List<Integer> clusterIds) throws IOException {
        return new ResourceClaim(etcd, maxGeneratorCount, clusterIds, Duration.ofMinutes(10));
    }

    /**
     * Claim a resource.
     *
     * @param etcd              Etcd connection.
     * @param maxGeneratorCount Maximum number of generators possible.
     * @param clusterIds        Cluster Ids available to use.
     * @param timeout           Time out if the process takes longer than this.
     * @return A resource claim.
     */
    public static ResourceClaim claim(Client etcd,
                                      int maxGeneratorCount,
                                      List<Integer> clusterIds,
                                      Duration timeout) throws IOException {
        return new ResourceClaim(etcd, maxGeneratorCount, clusterIds, timeout);
    }

    /**
     * Get the claimed resource.
     *
     * @return The resource claimed.
     * @throws IllegalStateException Thrown when the claim is no longer held.
     */
    public int getClusterId() {
        if (state != State.HAS_CLAIM) {
            throw new IllegalStateException("Resource claim not held.");
        }
        return clusterId;
    }

    public int getGeneratorId() {
        if (state != State.HAS_CLAIM) {
            throw new IllegalStateException("Resource claim not held.");
        }
        return generatorId;
    }

    /**
     * Relinquish the claim to this resource, and release it back to the resource pool.
     */
    public void close() {
        close(false);
    }

    public void close(boolean nodeAlreadyDeleted) {
        if (state == State.CLAIM_RELINQUISHED) {
            // Already relinquished nothing to do.
            return;
        }

        logger.debug("Closing resource-claim ({}).", resourceKey(clusterId, generatorId));

        // No need to delete the node if the reason we are closing is the deletion of said node.
        if (nodeAlreadyDeleted) {
            state = State.CLAIM_RELINQUISHED;
            return;
        }

        if (state == State.HAS_CLAIM) {
            state = State.CLAIM_RELINQUISHED;
            // Hang on to the claimed resource without using it for a short while to facilitate clock skew.
            // That is, if any participant is generating IDs with a slightly skewed clock, it can generate IDs that
            // overlap with the ones generated by the participant who successfully claims the same resource before or
            // after. By hanging on to each resource for a bit a slight clock skew may be handled gracefully.
            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    relinquishResource();
                }
                // Two seconds seems reasonable. The NTP docs state that clocks running more than 128ms out of sync are
                // rare under normal conditions.
            }, TimeUnit.SECONDS.toMillis(2));
        } else {
            state = State.CLAIM_RELINQUISHED;
        }

        for (Closeable closeable : closeables) {
            try {
                closeable.close();
            } catch (IOException e) {
                logger.warn("Failed to close resource properly.", e);
            }
        }
    }

    /**
     * Try to claim an available resource from the resource pool.
     *
     * @param etcd              Etcd connection.
     * @param maxGeneratorCount Maximum number of generators possible.
     * @param clusterIds        Cluster Ids available to use.
     * @param giveUpAfter       Give up after this instant in time.
     * @return The claimed resource.
     */
    ResourcePair claimResource(Client etcd, int maxGeneratorCount, List<Integer> clusterIds, Instant giveUpAfter)
            throws InterruptedException, IOException, ExecutionException {

        logger.debug("Trying to claim a resource.");

        int poolSize = maxGeneratorCount * clusterIds.size();

        GetOption getOptions = GetOption.builder()
                .withKeysOnly(true)
                .withRange(OptionsUtil.prefixEndOf(POOL_KEY))
                .build();
        GetResponse get = etcd.getKVClient().get(POOL_KEY, getOptions).get();

        List<ByteSequence> claimedResources = get.getKvs().stream()
                .map(KeyValue::getKey)
                .collect(Collectors.toList());

        if (claimedResources.size() >= poolSize) {
            logger.debug("No resources available at the moment (pool size: {}), waiting.", poolSize);
            // No resources available. Wait for a resource to become available.
            final CountDownLatch latch = new CountDownLatch(1);
            Watch.Watcher watcher = etcd.getWatchClient().watch(
                    POOL_KEY,
                    WatchOption
                            .builder()
                            .withRange(OptionsUtil.prefixEndOf(POOL_KEY))
                            .build(),
                    watchResponse -> latch.countDown()
            );
            awaitLatchUnlessItTakesTooLong(latch, giveUpAfter);
            watcher.close();
            return claimResource(etcd, maxGeneratorCount, clusterIds, giveUpAfter);
        }

        // Try to claim an available resource.
        for (Integer clusterId : clusterIds) {
            for (int generatorId = 0; generatorId < maxGeneratorCount; generatorId++) {
                String resourcePathString = resourceKey(clusterId, generatorId);
                ByteSequence resourcePath = ByteSequence.from(resourcePathString, StandardCharsets.UTF_8);
                if (!claimedResources.contains(resourcePath)) {
                    logger.debug("Trying to claim seemingly available resource {}.", resourcePathString);
                    TxnResponse txnResponse = etcd.getKVClient().txn()
                            .If(
                                    // Version == 0 means the key does not exist.
                                    new Cmp(resourcePath, Cmp.Op.EQUAL, CmpTarget.version(0))
                            ).Then(
                                    Op.put(
                                            resourcePath,
                                            ByteSequence.EMPTY,
                                            PutOption.builder().withLeaseId(leaseId).build()
                                    )
                            ).commit().get();

                    if (!txnResponse.isSucceeded()) {
                        // Failed to claim this resource for some reason.
                        continue;
                    }

                    closeables.add(etcd.getWatchClient().watch(resourcePath, watchResponse -> {
                        for (WatchEvent event : watchResponse.getEvents()) {
                            if (event.getEventType() == WatchEvent.EventType.DELETE) {
                                // Invalidate our claim when the node is deleted by some other process.
                                logger.debug("Resource-claim node unexpectedly deleted ({})", resourcePathString);
                                close(true);
                            }
                        }
                    }));

                    logger.debug("Successfully claimed resource {}.", resourcePathString);
                    return new ResourcePair(clusterId, generatorId);
                }
            }
        }

        return claimResource(etcd, maxGeneratorCount, clusterIds, giveUpAfter);
    }

    /**
     * Relinquish a claimed resource.
     */
    private void relinquishResource() {
        logger.debug("Relinquishing claimed resource {}:{}.", clusterId, generatorId);
        try {
            etcd.getLeaseClient().revoke(leaseId).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Failed to revoke Etcd lease.", e);
        }
    }

    static String resourceKey(Integer clusterId, int generatorId) {
        return POOL_PREFIX + clusterId + ":" + generatorId;
    }

    private void awaitLatchUnlessItTakesTooLong(CountDownLatch latch, Instant giveUpAfter)
            throws IOException, InterruptedException {
        if (giveUpAfter == null) {
            latch.await();
        } else {
            Instant now = Instant.now();
            if (!giveUpAfter.isAfter(now)) throw new IOException("Process timed out.");

            boolean success = latch.await(Duration.between(now, giveUpAfter).toMillis(), TimeUnit.MILLISECONDS);
            if (!success) {
                close();
                throw new IOException("Process timed out.");
            }
        }
    }

    /**
     * Internal state of this ResourceClaim.
     */
    public enum State {
        INITIALIZING,
        HAS_CLAIM,
        CLAIM_RELINQUISHED
    }
}

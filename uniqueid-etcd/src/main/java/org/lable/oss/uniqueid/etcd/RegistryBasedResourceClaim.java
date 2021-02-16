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
package org.lable.oss.uniqueid.etcd;

import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.lease.LeaseGrantResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.options.WatchOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class RegistryBasedResourceClaim {
    private static final Logger logger = LoggerFactory.getLogger(RegistryBasedResourceClaim.class);

    static final String REGISTRY_PREFIX = "registry/";
    static final ByteSequence REGISTRY_KEY = ByteSequence.from(REGISTRY_PREFIX, StandardCharsets.UTF_8);
    static final ByteSequence LOCK_NAME = ByteSequence.from("unique-id-registry-lock", StandardCharsets.UTF_8);

    final Supplier<Client> connectToEtcd;
    final String registryEntry;
    final int clusterId;
    final int generatorId;

    final int poolSize;

    RegistryBasedResourceClaim(Supplier<Client> connectToEtcd, int maxGeneratorCount, String registryEntry)
            throws IOException {
        this.registryEntry = registryEntry;
        this.connectToEtcd = connectToEtcd;

        logger.debug("Acquiring resource-claim…");

        Client etcd = connectToEtcd.get();

        List<Integer> clusterIds = ClusterID.get(etcd);

        Duration timeout = Duration.ofMinutes(5);
        Instant giveUpAfter = Instant.now().plus(timeout);

        this.poolSize = maxGeneratorCount;

        try {
            LeaseGrantResponse lease = etcd.getLeaseClient().grant(5).get();
            long leaseId = lease.getID();

            // Keep the lease alive until we are done.
            CloseableClient leaseKeepAlive = EtcdHelper.keepLeaseAlive(etcd, leaseId, null);

            // Release the lease when closed.

            // Acquire the lock. This makes sure we are the only process claiming a resource.
            LockResponse lock;
            try {
                lock = etcd.getLockClient()
                        .lock(LOCK_NAME, leaseId)
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
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

            leaseKeepAlive.close();
        } catch (ExecutionException e) {
            close();
            throw new IOException(e);
        } catch (InterruptedException e) {
            close();
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }

        logger.debug("Resource-claim acquired ({}/{}).", clusterId, generatorId);
    }

    public static RegistryBasedResourceClaim claim(Supplier<Client> connectToEtcd,
                                                   int maxGeneratorCount,
                                                   String registryEntry) throws IOException {
        return new RegistryBasedResourceClaim(connectToEtcd, maxGeneratorCount, registryEntry);
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

        int registrySize = maxGeneratorCount * clusterIds.size();

        GetOption getOptions = GetOption.newBuilder()
                .withKeysOnly(true)
                .withPrefix(REGISTRY_KEY)
                .build();
        GetResponse get = etcd.getKVClient().get(REGISTRY_KEY, getOptions).get();

        List<ByteSequence> claimedResources = get.getKvs().stream()
                .map(KeyValue::getKey)
                .collect(Collectors.toList());

        if (claimedResources.size() >= registrySize) {
            logger.debug("No resources available at the moment (registry size: {}), waiting.", registrySize);
            // No resources available. Wait for a resource to become available.
            final CountDownLatch latch = new CountDownLatch(1);
            Watch.Watcher watcher = etcd.getWatchClient().watch(
                    REGISTRY_KEY,
                    WatchOption.newBuilder().withPrefix(REGISTRY_KEY).build(),
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
                                            ByteSequence.from(registryEntry, StandardCharsets.UTF_8),
                                            PutOption.newBuilder().build()
                                    )
                            ).commit().get();

                    if (!txnResponse.isSucceeded()) {
                        // Failed to claim this resource for some reason.
                        continue;
                    }

                    logger.debug("Successfully claimed resource {}.", resourcePathString);
                    return new ResourcePair(clusterId, generatorId);
                }
            }
        }

        return claimResource(etcd, maxGeneratorCount, clusterIds, giveUpAfter);
    }

    static String resourceKey(Integer clusterId, int generatorId) {
        return REGISTRY_PREFIX + clusterId + ":" + generatorId;
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
     * Relinquish a claimed resource.
     */
    private void relinquishResource() {
        logger.debug("Relinquishing claimed registry resource {}:{}.", clusterId, generatorId);

        Client etcd = connectToEtcd.get();
        String resourcePathString = resourceKey(clusterId, generatorId);
        ByteSequence resourcePath = ByteSequence.from(resourcePathString, StandardCharsets.UTF_8);

        try {
            etcd.getKVClient().delete(resourcePath).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            logger.error("Failed to revoke Etcd lease.", e);
        }
    }

    public int getClusterId() {
        return clusterId;
    }

    public int getGeneratorId() {
        return generatorId;
    }

    public void close() {
        relinquishResource();
    }

    public String getRegistryEntry() {
        return registryEntry;
    }
}

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

import io.etcd.jetcd.*;
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
    final KV kvClient;

    RegistryBasedResourceClaim(Supplier<Client> connectToEtcd,
                               int maxGeneratorCount,
                               String registryEntry,
                               Duration acquisitionTimeout,
                               boolean waitWhenNoResourcesAvailable)
            throws IOException {
        this.registryEntry = registryEntry;
        this.connectToEtcd = connectToEtcd;

        logger.info("Acquiring resource-claim…");

        Client etcd = connectToEtcd.get();

        // Keep the KV client around, because if we try to instantiate it during shutdown of a Java application when
        // the resource is likely to be released, it will fail because further down the stack an attempt is made to
        // register a shutdown handler, which fails because the application is already shutting down. So we instantiate
        // this here and keep it.
        kvClient = etcd.getKVClient();

        List<Integer> clusterIds = ClusterID.get(etcd);

        Duration timeout = acquisitionTimeout == null
                ? Duration.ofMinutes(5)
                : acquisitionTimeout;
        Instant giveUpAfter = Instant.now().plus(timeout);

        this.poolSize = maxGeneratorCount;

        ResourcePair resourcePair = null;
        try {
            logger.debug("Acquiring lock, timeout is set to {}.", timeout);
            // Have the lease TTL just a bit after our timeout.
            LeaseGrantResponse lease = etcd.getLeaseClient().grant(timeout.plusSeconds(5).getSeconds()).get();
            long leaseId = lease.getID();

            // Acquire the lock. This makes sure we are the only process claiming a resource.
            LockResponse lock;
            try {
                lock = etcd.getLockClient()
                        .lock(LOCK_NAME, leaseId)
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new IOException("Process timed out.");
            }

            // Keep the lease alive for another period in order to safely finish claiming the resource.
            etcd.getLeaseClient().keepAliveOnce(leaseId).get();

            if (logger.isDebugEnabled()) {
                logger.debug("Acquired lock: {}.", lock.getKey().toString(StandardCharsets.UTF_8));
            }

            resourcePair = claimResource(
                    etcd, maxGeneratorCount, clusterIds, giveUpAfter, waitWhenNoResourcesAvailable
            );
            this.clusterId = resourcePair.clusterId;
            this.generatorId = resourcePair.generatorId;

            // Explicitly release the lock. If this line is not reached due to exceptions raised, the lock will
            // automatically be removed when the lease holding it expires.
            etcd.getLockClient().unlock(lock.getKey()).get();
            if (logger.isDebugEnabled()) {
                logger.debug("Released lock: {}.", lock.getKey().toString(StandardCharsets.UTF_8));
            }

            // Revoke the lease instead of letting it time out.
            etcd.getLeaseClient().revoke(leaseId).get();
        } catch (ExecutionException e) {
            if (resourcePair != null) {
                close();
            }
            throw new IOException(e);
        } catch (InterruptedException e) {
            if (resourcePair != null) {
                close();
            }
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }

        logger.debug("Resource-claim acquired ({}/{}).", clusterId, generatorId);
    }

    /**
     * Claim a resource.
     *
     * @param connectToEtcd                Provide a connection to Etcd.
     * @param maxGeneratorCount            Maximum number of generators possible.
     * @param registryEntry                Metadata stored under the Etcd key.
     * @param acquisitionTimeout           Abort attempt to claim a resource after this duration.
     * @param waitWhenNoResourcesAvailable Wait for a resource to become available when all resources are claimed.
     * @return The resource claim, if successful.
     * @throws IOException Thrown when the claim could not be acquired.
     */
    public static RegistryBasedResourceClaim claim(Supplier<Client> connectToEtcd,
                                                   int maxGeneratorCount,
                                                   String registryEntry,
                                                   Duration acquisitionTimeout,
                                                   boolean waitWhenNoResourcesAvailable) throws IOException {
        return new RegistryBasedResourceClaim(
                connectToEtcd, maxGeneratorCount, registryEntry, acquisitionTimeout, waitWhenNoResourcesAvailable
        );
    }

    /**
     * Try to claim an available resource from the resource pool.
     *
     * @param etcd                         Etcd connection.
     * @param maxGeneratorCount            Maximum number of generators possible.
     * @param clusterIds                   Cluster Ids available to use.
     * @param giveUpAfter                  Give up after this instant in time.
     * @param waitWhenNoResourcesAvailable Wait for a resource to become available when all resources are claimed.
     * @return The claimed resource.
     */
    ResourcePair claimResource(Client etcd,
                               int maxGeneratorCount,
                               List<Integer> clusterIds,
                               Instant giveUpAfter,
                               boolean waitWhenNoResourcesAvailable)
            throws InterruptedException, IOException, ExecutionException {

        logger.debug("Trying to claim a resource.");

        int registrySize = maxGeneratorCount * clusterIds.size();

        GetOption getOptions = GetOption.builder()
                .withKeysOnly(true)
                .withRange(OptionsUtil.prefixEndOf(REGISTRY_KEY))
                .build();
        GetResponse get = etcd.getKVClient().get(REGISTRY_KEY, getOptions).get();

        List<ByteSequence> claimedResources = get.getKvs().stream()
                .map(KeyValue::getKey)
                .collect(Collectors.toList());

        if (claimedResources.size() >= registrySize) {
            if (!waitWhenNoResourcesAvailable) {
                throw new IOException(
                        "No resources available. Giving up as requested. Registry size: " + registrySize + "."
                );
            }
            logger.warn("No resources available at the moment (registry size: {}), waiting.", registrySize);
            // No resources available. Wait for a resource to become available.
            final CountDownLatch latch = new CountDownLatch(1);
            Watch.Watcher watcher = etcd.getWatchClient().watch(
                    REGISTRY_KEY,
                    WatchOption.builder()
                            .withRange(OptionsUtil.prefixEndOf(REGISTRY_KEY))
                            .build(),
                    watchResponse -> latch.countDown()
            );
            awaitLatchUnlessItTakesTooLong(latch, giveUpAfter);
            watcher.close();
            return claimResource(etcd, maxGeneratorCount, clusterIds, giveUpAfter, true);
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
                                            PutOption.builder().build()
                                    )
                            ).commit().get();

                    if (!txnResponse.isSucceeded()) {
                        // Failed to claim this resource for some reason.
                        continue;
                    }

                    logger.info("Successfully claimed resource {}.", resourcePathString);
                    return new ResourcePair(clusterId, generatorId);
                }
            }
        }

        return claimResource(etcd, maxGeneratorCount, clusterIds, giveUpAfter, waitWhenNoResourcesAvailable);
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

        String resourcePathString = resourceKey(clusterId, generatorId);
        ByteSequence resourcePath = ByteSequence.from(resourcePathString, StandardCharsets.UTF_8);

        try {
            kvClient.delete(resourcePath).get();
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

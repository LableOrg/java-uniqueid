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
import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.GeneratorIdentityHolder;
import org.lable.oss.uniqueid.bytes.Blueprint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;

public class SynchronizedGeneratorIdentity implements GeneratorIdentityHolder {
    private static final Logger logger = LoggerFactory.getLogger(SynchronizedGeneratorIdentity.class);

    private final Client client;
    private final List<Integer> clusterIds;
    private final Supplier<Duration> claimDurationSupplier;
    private final Supplier<Duration> acquisitionTimeoutSupplier;

    ResourceClaim resourceClaim = null;

    public SynchronizedGeneratorIdentity(Client client,
                                         List<Integer> clusterIds,
                                         Supplier<Duration> claimDurationSupplier,
                                         Supplier<Duration> acquisitionTimeoutSupplier) {
        this.client = client;
        this.clusterIds = clusterIds;
        this.claimDurationSupplier = claimDurationSupplier;
        this.acquisitionTimeoutSupplier = acquisitionTimeoutSupplier == null
                ? () -> null
                : acquisitionTimeoutSupplier;
    }

    /**
     * Create a new {@link SynchronizedGeneratorIdentity} instance.
     * <p>
     * By using a {@link Supplier} instead of static longs for the claim duration and the acquisition timeout, these
     * values can be dynamically reconfigured at runtime.
     *
     * @param endpoints                  Addresses of the Etcd cluster (comma-separated).
     * @param namespace                  Namespace of the unique-id keys in Etcd.
     * @param claimDurationSupplier      Provides the amount of time a claim to a generator-ID should be held.
     * @param acquisitionTimeoutSupplier Provides the amount of time the process of acquiring a generator-ID may take.
     *                                   May be {@code null} to indicate that the process may wait indefinitely.
     * @return A {@link SynchronizedGeneratorIdentity} instance.
     */
    public static SynchronizedGeneratorIdentity basedOn(String endpoints,
                                                        String namespace,
                                                        Supplier<Duration> claimDurationSupplier,
                                                        Supplier<Duration> acquisitionTimeoutSupplier)
            throws IOException {
        Client client = Client.builder()
                .endpoints(endpoints.split(","))
                .namespace(ByteSequence.from(namespace, StandardCharsets.UTF_8))
                .build();
        List<Integer> clusterIds = ClusterID.get(client);

        return new SynchronizedGeneratorIdentity(
                client, clusterIds, claimDurationSupplier, acquisitionTimeoutSupplier
        );
    }

    /**
     * Create a new {@link SynchronizedGeneratorIdentity} instance.
     *
     * @param endpoints     Addresses of the Etcd cluster (comma-separated).
     * @param namespace     Namespace of the unique-id keys in Etcd.
     * @param claimDuration How long a claim to a generator-ID should be held, in milliseconds.
     * @return A {@link SynchronizedGeneratorIdentity} instance.
     */
    public static SynchronizedGeneratorIdentity basedOn(String endpoints,
                                                        String namespace,
                                                        Long claimDuration)
            throws IOException {
        Client client = Client.builder()
                .endpoints(endpoints.split(","))
                .namespace(ByteSequence.from(namespace, StandardCharsets.UTF_8))
                .build();
        List<Integer> clusterIds = ClusterID.get(client);
        Supplier<Duration> durationSupplier = () -> Duration.ofMillis(claimDuration);

        return new SynchronizedGeneratorIdentity(client, clusterIds, durationSupplier, null);
    }

    @Override
    public int getClusterId() throws GeneratorException {
        acquireResourceClaim();

        try {
            return resourceClaim.getClusterId();
        } catch (IllegalStateException e) {
            // Claim expired?
            relinquishResourceClaim();
            acquireResourceClaim();
            return resourceClaim.getClusterId();
        }
    }

    @Override
    public int getGeneratorId() throws GeneratorException {
        acquireResourceClaim();

        try {
            return resourceClaim.getGeneratorId();
        } catch (IllegalStateException e) {
            // Claim expired?
            relinquishResourceClaim();
            acquireResourceClaim();
            return resourceClaim.getGeneratorId();
        }
    }


    public synchronized void relinquishResourceClaim() {
        if (resourceClaim == null) return;
        resourceClaim.close();
        resourceClaim = null;
    }

    private synchronized void acquireResourceClaim() throws GeneratorException {
        if (resourceClaim != null) return;

        resourceClaim = acquireResourceClaim(0);
    }

    private ResourceClaim acquireResourceClaim(int retries) throws GeneratorException {
        try {
            return ExpiringResourceClaim.claimExpiring(
                    client,
                    Blueprint.MAX_GENERATOR_ID + 1,
                    clusterIds,
                    claimDurationSupplier == null ? null : claimDurationSupplier.get(),
                    acquisitionTimeoutSupplier.get()
            );
        } catch (IOException e) {
            logger.warn(
                    "Connection to Etcd failed, retrying resource claim acquisition, attempt {}.",
                    retries + 1
            );
            if (retries < 3) {
                return acquireResourceClaim(retries + 1);
            } else {
                throw new GeneratorException(e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (resourceClaim != null) {
            resourceClaim.close();
        }
    }
}

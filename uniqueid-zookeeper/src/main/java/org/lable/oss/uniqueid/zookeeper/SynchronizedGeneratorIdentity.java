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

import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.GeneratorIdentityHolder;
import org.lable.oss.uniqueid.bytes.Blueprint;
import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnection;

import java.io.IOException;
import java.time.Duration;
import java.util.function.Supplier;

/**
 * Implementation of {@link GeneratorIdentityHolder} that holds and reacquires when necessary a claim to a
 * generator-ID. When {@link #getGeneratorId()} is called the generator-ID currently held by this object will be
 * returned. There is no guarantee that the same ID is returned on subsequent calls. Generator IDs should not be
 * cached or reused.
 *
 * @see #basedOn(String, String, Long)
 * @see #basedOn(String, String, DurationSupplier)
 */
public class SynchronizedGeneratorIdentity implements GeneratorIdentityHolder {
    final int clusterId;
    final DurationSupplier claimDurationSupplier;
    final String zNode;
    final ZooKeeperConnection zooKeeperConnection;

    ResourceClaim resourceClaim = null;

    public SynchronizedGeneratorIdentity(ZooKeeperConnection zooKeeperConnection,
                                         String zNode,
                                         int clusterId,
                                         DurationSupplier claimDurationSupplier) {
        this.zooKeeperConnection = zooKeeperConnection;
        this.zNode = zNode;
        this.clusterId = clusterId;
        this.claimDurationSupplier = claimDurationSupplier;
    }

    /**
     * Create a new {@link SynchronizedGeneratorIdentity} instance.
     *
     * @param quorum                Addresses of the ZooKeeper quorum (comma-separated).
     * @param znode                 Root znode of the ZooKeeper resource-pool.
     * @param claimDurationSupplier Provides the amount of time a claim to a generator-ID should be held. By using a
     *                              {@link DurationSupplier} instead of a static long, this may dynamically reconfigured
     *                              at runtime.
     * @return A {@link SynchronizedGeneratorIdentity} instance.
     */
    public static SynchronizedGeneratorIdentity basedOn(String quorum,
                                                        String znode,
                                                        DurationSupplier claimDurationSupplier)
            throws IOException {
        ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection(quorum);
        int clusterId = ClusterID.get(zooKeeperConnection.get(), znode);

        return new SynchronizedGeneratorIdentity(zooKeeperConnection, znode, clusterId, claimDurationSupplier);
    }

    /**
     * Create a new {@link SynchronizedGeneratorIdentity} instance.
     *
     * @param quorum        Addresses of the ZooKeeper quorum (comma-separated).
     * @param znode         Root znode of the ZooKeeper resource-pool.
     * @param claimDuration How long a claim to a generator-ID should be held, in milliseconds.
     * @return A {@link SynchronizedGeneratorIdentity} instance.
     */
    public static SynchronizedGeneratorIdentity basedOn(String quorum,
                                                        String znode,
                                                        final Long claimDuration)
            throws IOException {
        ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection(quorum);
        int clusterId = ClusterID.get(zooKeeperConnection.get(), znode);
        DurationSupplier durationSupplier = new DurationSupplier() {
            @Override
            public Long get() {
                return claimDuration;
            }
        };

        return new SynchronizedGeneratorIdentity(zooKeeperConnection, znode, clusterId, durationSupplier);
    }

    @Override
    public int getClusterId() throws GeneratorException {
        return clusterId;
    }

    @Override
    public int getGeneratorId() throws GeneratorException {
        if (resourceClaim == null) {
            resourceClaim = acquireResourceClaim();
        }

        try {
            return resourceClaim.get();
        } catch (IllegalStateException e) {
            // Claim expired?
            resourceClaim.close();
            resourceClaim = acquireResourceClaim();
            return resourceClaim.get();
        }
    }

    public String getZNode() {
        return zNode;
    }

    public void relinquishGeneratorIdClaim() {
        resourceClaim.close();
        resourceClaim = null;
    }

    private ResourceClaim acquireResourceClaim() throws GeneratorException {
        Long claimDuration = getDurationInMillis(claimDurationSupplier);
        try {
            return ExpiringResourceClaim.claimExpiring(
                    zooKeeperConnection,
                    Blueprint.MAX_GENERATOR_ID + 1,
                    zNode,
                    claimDuration
            );
        } catch (IOException e) {
            throw new GeneratorException(e);
        }
    }

    @Override
    public void close() throws IOException {
        if (resourceClaim != null) {
            resourceClaim.close();
        }
    }

    static Long getDurationInMillis(DurationSupplier durationSupplier) {
        if (durationSupplier == null) return null;
        return durationSupplier.get();
    }
}

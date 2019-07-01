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
package org.lable.oss.uniqueid;

import org.lable.oss.uniqueid.bytes.Blueprint;
import org.lable.oss.uniqueid.bytes.Mode;

import java.util.HashMap;
import java.util.Map;

import static org.lable.oss.uniqueid.ParameterUtil.assertParameterWithinBounds;

/**
 * Create an {@link IDGenerator} that generates short, possibly unique IDs based on the current timestamp. Whether the
 * IDs are truly unique or not depends on the scope of use; if the combination of generator-ID and cluster-ID passed
 * to this class is unique (i.e., there is only one ID-generator using that specific combination of generator-ID and
 * cluster-ID within the confines of your computing environment at the moment you generate an ID) then the IDs
 * returned are unique.
 */
public class LocalUniqueIDGeneratorFactory {
    final static Map<String, IDGenerator> instances = new HashMap<>();

    /**
     * Return the UniqueIDGenerator instance for this specific generator-ID, cluster-ID combination. If one was
     * already created, that is returned.
     *
     * @param generatorId Generator ID to use (0 ≤ n ≤ 255).
     * @param clusterId   Cluster ID to use (0 ≤ n ≤ 15).
     * @param clock       Clock implementation.
     * @param mode        Generator mode.
     * @return A thread-safe UniqueIDGenerator instance.
     */
    public synchronized static IDGenerator generatorFor(int generatorId, int clusterId, Clock clock, Mode mode) {
        assertParameterWithinBounds("generatorId", 0, Blueprint.MAX_GENERATOR_ID, generatorId);
        assertParameterWithinBounds("clusterId", 0, Blueprint.MAX_CLUSTER_ID, clusterId);
        String generatorAndCluster = String.format("%d_%d", generatorId, clusterId);
        if (!instances.containsKey(generatorAndCluster)) {
            GeneratorIdentityHolder identityHolder = LocalGeneratorIdentity.with(clusterId, generatorId);
            instances.putIfAbsent(generatorAndCluster, new BaseUniqueIDGenerator(identityHolder, clock, mode));
        }
        return instances.get(generatorAndCluster);
    }

    /**
     * Return the UniqueIDGenerator instance for this specific generator-ID, cluster-ID combination. If one was
     * already created, that is returned.
     *
     * @param generatorId Generator ID to use (0 ≤ n ≤ 255).
     * @param clusterId   Cluster ID to use (0 ≤ n ≤ 15).
     * @param mode        Generator mode.
     * @return A thread-safe UniqueIDGenerator instance.
     */
    public synchronized static IDGenerator generatorFor(int generatorId, int clusterId, Mode mode) {
        return generatorFor(generatorId, clusterId, null, mode);
    }
}

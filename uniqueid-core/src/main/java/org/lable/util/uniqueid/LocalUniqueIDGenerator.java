package org.lable.util.uniqueid;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Generate short, possibly unique ID's based on the current timestamp. Whether the ID's are truly unique or not
 * depends on the scope of its use. If the combination of generator-ID and cluster-ID passed to this class is unique —
 * i.e., there is only one ID-generator using that specific combination of generator-ID and cluster-ID within the
 * confines of your computing environment at the moment you generate an ID — then the ID's returned are unique.
 */
public class LocalUniqueIDGenerator extends BaseUniqueIDGenerator {

    final static ConcurrentMap<String, BaseUniqueIDGenerator> instances =
            new ConcurrentHashMap<String, BaseUniqueIDGenerator>();

    /**
     * Create a new UniqueIDGenerator instance.
     *
     * @param generatorId Generator ID to use (0 <= n < 64).
     * @param clusterId   Cluster ID to use (0 <= n < 16).
     */
    protected LocalUniqueIDGenerator(int generatorId, int clusterId) {
        super(generatorId, clusterId);
    }

    /**
     * Return the UniqueIDGenerator instance for this specific generator-ID, cluster-ID combination.
     *
     * @param generatorId Generator ID to use (0 <= n < 64).
     * @param clusterId   Cluster ID to use (0 <= n < 16).
     * @return A thread-safe UniqueIDGenerator instance.
     */
    public synchronized static IDGenerator generatorFor(int generatorId, int clusterId) {
        assertParameterWithinBounds("generator-ID", 0, MAX_GENERATOR_ID, generatorId);
        assertParameterWithinBounds("cluster-ID", 0, MAX_CLUSTER_ID, clusterId);

        String generatorAndCluster = String.format("%d_%d", generatorId, clusterId);
        if (!instances.containsKey(generatorAndCluster)) {
            instances.putIfAbsent(generatorAndCluster, new LocalUniqueIDGenerator(generatorId, clusterId));
        }
        return instances.get(generatorAndCluster);
    }
}

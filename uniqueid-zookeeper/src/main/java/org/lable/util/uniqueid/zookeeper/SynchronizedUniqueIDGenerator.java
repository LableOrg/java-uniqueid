package org.lable.util.uniqueid.zookeeper;

import org.lable.util.uniqueid.GeneratorException;
import org.lable.util.uniqueid.UniqueIDGenerator;

import java.io.IOException;

/**
 * A {@link org.lable.util.uniqueid.UniqueIDGenerator} that coordinates the generator ID used by other processes
 * using this class via the same ZooKeeper quorum by attempting to claim an available ID for itself.
 * <p/>
 * Although claims on a generator ID will be automatically relinquished after the connection to the ZooKeeper quorum
 * is lost, instances of this class should be explicitly closed after use, if you do not expect to generate anymore
 * ID's at that time.
 * <p/>
 * Because claimed generator ID's are automatically returned to the pool after a set time
 * ({@link org.lable.util.uniqueid.zookeeper.ExpiringResourceClaim#DEFAULT_TIMEOUT}), there is no guarantee that a
 *
 * @see #close()
 */
public class SynchronizedUniqueIDGenerator extends UniqueIDGenerator {

    ResourceClaim resourceClaim;
    final int poolSize;

    static SynchronizedUniqueIDGenerator instance = null;

    /**
     * Create a new SynchronizedUniqueIDGenerator instance.
     *
     * @param resourceClaim Resource claim for a generator ID.
     * @param clusterId     Cluster ID to use (0 <= n < 16).
     */
    SynchronizedUniqueIDGenerator(ResourceClaim resourceClaim, int clusterId) {
        super(resourceClaim.get(), clusterId);
        this.poolSize = resourceClaim.poolSize;
        this.resourceClaim = resourceClaim;
    }

    /**
     * Get the synchronized ID generator instance.
     *
     * @return An instance of this class.
     * @throws IOException Thrown when something went wrong trying to find the cluster ID or trying to claim a
     *                     generator ID.
     */
    public static SynchronizedUniqueIDGenerator generator() throws IOException {
        int clusterId = ClusterID.get(ZooKeeperConnection.get());
        assertParameterWithinBounds("cluster-ID", 0, MAX_CLUSTER_ID, clusterId);

        if (instance == null) {
            int poolSize = UniqueIDGenerator.MAX_GENERATOR_ID + 1;
            ResourceClaim resourceClaim = ResourceClaim.claim(ZooKeeperConnection.get(), poolSize);
            instance = new SynchronizedUniqueIDGenerator(resourceClaim, clusterId);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] generate() throws GeneratorException {
        try {
            generatorId = resourceClaim.get();
        } catch (IllegalStateException e) {
            // Claim expired?
            resourceClaim.close();
            try {
                resourceClaim = ResourceClaim.claim(ZooKeeperConnection.get(), poolSize);
            } catch (IOException e1) {
                throw new GeneratorException(e);
            }
            generatorId = resourceClaim.get();
        }
        return super.generate();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        resourceClaim.close();
    }
}

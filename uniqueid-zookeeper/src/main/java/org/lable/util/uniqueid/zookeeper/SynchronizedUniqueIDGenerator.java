package org.lable.util.uniqueid.zookeeper;

import org.lable.util.uniqueid.GeneratorException;
import org.lable.util.uniqueid.UniqueIDGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * ({@link org.lable.util.uniqueid.zookeeper.ExpiringResourceClaim#DEFAULT_TIMEOUT}),
 * there is no guarantee that ID's generated have the same generator ID.
 */
public class SynchronizedUniqueIDGenerator extends UniqueIDGenerator {
    final static Logger logger = LoggerFactory.getLogger(SynchronizedUniqueIDGenerator.class);

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
    public static synchronized SynchronizedUniqueIDGenerator generator() throws IOException {
        if (instance == null) {
            int clusterId = ClusterID.get(ZooKeeperConnection.get());
            assertParameterWithinBounds("cluster-ID", 0, MAX_CLUSTER_ID, clusterId);
            logger.debug("Creating new instance.");
            int poolSize = UniqueIDGenerator.MAX_GENERATOR_ID + 1;
            ResourceClaim resourceClaim = ExpiringResourceClaim.claim(ZooKeeperConnection.get(), poolSize);
            instance = new SynchronizedUniqueIDGenerator(resourceClaim, clusterId);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized byte[] generate() throws GeneratorException {
        try {
            generatorId = resourceClaim.get();
        } catch (IllegalStateException e) {
            // Claim expired?
            resourceClaim.close();
            try {
                resourceClaim = ExpiringResourceClaim.claim(ZooKeeperConnection.get(), poolSize);
            } catch (IOException ioe) {
                throw new GeneratorException(ioe);
            }
            generatorId = resourceClaim.get();
        }
        return super.generate();
    }

    /**
     * Return the claimed generator ID to the pool. Call this when you are done generating IDs. If you don't the
     * claim will expire automatically, but this takes a while.
     */
    public void relinquishGeneratorIDClaim() {
        resourceClaim.close();
    }
}

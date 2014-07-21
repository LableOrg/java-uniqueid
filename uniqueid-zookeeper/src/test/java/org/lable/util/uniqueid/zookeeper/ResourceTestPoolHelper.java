package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * Static methods for filling a ZooKeeper testing tree.
 */
public class ResourceTestPoolHelper {

    /**
     * Create the two znodes used for the queue and the resource pool.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void prepareEmptyQueueAndPool(ZooKeeper zookeeper)
            throws KeeperException, InterruptedException {
        try {
            zookeeper.create("/unique-id-generator", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }
        zookeeper.create(ResourceClaim.QUEUE_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zookeeper.create(ResourceClaim.POOL_NODE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * Create the cluster ID znode.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param clusterId Cluster ID to configure.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void prepareClusterID(ZooKeeper zookeeper, int clusterId)
            throws KeeperException, InterruptedException {
        try {
            zookeeper.create("/unique-id-generator", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }
        zookeeper.create("/unique-id-generator/cluster-id", String.valueOf(clusterId).getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
}

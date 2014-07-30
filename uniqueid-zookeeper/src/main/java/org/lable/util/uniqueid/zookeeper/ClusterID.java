package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

import static org.lable.util.uniqueid.zookeeper.ZooKeeperHelper.create;
import static org.lable.util.uniqueid.zookeeper.ZooKeeperHelper.mkdirp;

/**
 * Retrieves the numeric cluster ID from the ZooKeeper quorum.
 */
public class ClusterID {
    final static String CLUSTER_ID_NODE = "/cluster-id";
    final static int DEFAULT_CLUSTER_ID = 0;

    /**
     * Retrieves the numeric cluster ID from the ZooKeeper quorum.
     *
     * @param zookeeper ZooKeeper instance to work with.
     * @return The cluster ID, if configured in the quorum.
     * @throws IOException                     Thrown when retrieving the ID fails.
     * @throws java.lang.NumberFormatException Thrown when the supposed ID found in the quorum could not be parsed as
     *                                         an integer.
     */
    public static int get(ZooKeeper zookeeper, String znode) throws IOException {
        try {
            Stat stat = zookeeper.exists(znode + CLUSTER_ID_NODE, false);
            if (stat == null) {
                mkdirp(zookeeper, znode);
                create(zookeeper, znode + CLUSTER_ID_NODE, String.valueOf(DEFAULT_CLUSTER_ID).getBytes());
            }

            byte[] id = zookeeper.getData(znode + CLUSTER_ID_NODE, false, null);
            return Integer.valueOf(new String(id));
        } catch (KeeperException e) {
            throw new IOException(String.format("Failed to retrieve the cluster ID from the ZooKeeper quorum. " +
                    "Expected to find it at znode %s.", znode + CLUSTER_ID_NODE), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}

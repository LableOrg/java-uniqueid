package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Retrieves the numeric cluster ID from the ZooKeeper quorum.
 */
public class ClusterID {
    final static String CLUSTER_ID_NODE = "/unique-id-generator/cluster-id";

    /**
     * @param zookeeper ZooKeeper instance to work with.
     * @return The cluster ID, if configured in the quorum.
     * @throws IOException                     Thrown when retrieving the ID fails.
     * @throws java.lang.NumberFormatException Thrown when the supposed ID found in the quorum could not be parsed as
     *                                         an integer.
     */
    public static int get(ZooKeeper zookeeper) throws IOException {
        try {
            byte[] id = zookeeper.getData(CLUSTER_ID_NODE, false, null);
            return Integer.valueOf(new String(id));
        } catch (KeeperException e) {
            throw new IOException(String.format("Failed to retrieve the cluster ID from the ZooKeeper quorum. " +
                    "Expected to find it at znode %s.", CLUSTER_ID_NODE), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}

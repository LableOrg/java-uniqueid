package org.lable.util.uniqueid.zookeeper;


import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.lable.util.uniqueid.zookeeper.ResourcePoolHelper.prepareClusterID;

public class ClusterIDIT {

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    final static int CLUSTER_ID = 15;

    @Test
    public void getClusterIDTest() throws Exception {
        ZooKeeper zookeeper = zkInstance.getZookeeperConnection();
        prepareClusterID(zookeeper, CLUSTER_ID);

        int id = ClusterID.get(zookeeper);
        assertThat(id, is(CLUSTER_ID));

        ZooKeeperConnection.reset();
    }

    @Test(expected = IOException.class)
    public void failToGetClusterIDTest() throws Exception {
        // Should fail because the /unique-id-generator/cluster-id znode is missing.
        ClusterID.get(zkInstance.getZookeeperConnection());
    }
}
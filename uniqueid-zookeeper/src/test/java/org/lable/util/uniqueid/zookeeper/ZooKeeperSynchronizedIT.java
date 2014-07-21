package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.lable.util.uniqueid.UniqueIDGenerator;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.lable.util.uniqueid.zookeeper.ResourcePoolHelper.prepareClusterID;
import static org.lable.util.uniqueid.zookeeper.ResourcePoolHelper.prepareEmptyQueueAndPool;

public class ZooKeeperSynchronizedIT {

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    final static int CLUSTER_ID = 4;

    @Before
    public void before() throws Exception {
        ZooKeeperConnection.configure(zkInstance.getQuorumAddresses());
        ZooKeeperConnection.reset();
        ZooKeeper zookeeper = ZooKeeperConnection.get();
        prepareEmptyQueueAndPool(zookeeper);
        prepareClusterID(zookeeper, CLUSTER_ID);
    }

    @Test
    public void simpleTest() throws Exception {
        UniqueIDGenerator generator = SynchronizedUniqueIDGenerator.generator();
        byte[] result = generator.generate();
        UniqueIDGenerator.Blueprint blueprint = UniqueIDGenerator.parse(result);
        assertThat(result.length, is(8));
        assertThat(blueprint.getClusterId(), is(CLUSTER_ID));
    }
}

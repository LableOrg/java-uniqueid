package org.lable.oss.uniqueid.zookeeper;

import net.ggtools.junit.categories.Slow;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnection;


import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.lable.oss.uniqueid.zookeeper.ResourceTestPoolHelper.prepareClusterID;
import static org.lable.oss.uniqueid.zookeeper.ResourceTestPoolHelper.prepareEmptyQueueAndPool;

@Category(Slow.class)
public class ExpirationZooKeeperSynchronizedIT {

    String zookeeperQuorum;
    String znode = "/unique-id-generator";

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    final static int CLUSTER_ID = 8;

    @Before
    public void before() throws Exception {
        zookeeperQuorum = zkInstance.getQuorumAddresses();
        ZooKeeperConnection.configure(zookeeperQuorum);
        ZooKeeperConnection.reset();
        ZooKeeper zookeeper = ZooKeeperConnection.get();
        prepareEmptyQueueAndPool(zookeeper, znode);
        prepareClusterID(zookeeper, znode, CLUSTER_ID);
    }

    @Test
    public void testExpirationOfResourceClaimTest() throws Exception {
        SynchronizedUniqueIDGenerator generator = SynchronizedUniqueIDGenerator.generatorFor(zookeeperQuorum, znode);
        generator.generate();
        int claim1 = generator.resourceClaim.hashCode();

        // Wait for the resource claim to expire.
        TimeUnit.SECONDS.sleep(40);

        generator.generate();
        int claim2 = generator.resourceClaim.hashCode();

        // Prove that a new ResourceClaim instance was created after the first one timed out.
        assertThat(claim1, is(not(claim2)));
    }
}

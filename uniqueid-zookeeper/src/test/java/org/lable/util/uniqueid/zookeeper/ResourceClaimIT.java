package org.lable.util.uniqueid.zookeeper;


import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.junit.Assert.assertThat;
import static org.lable.util.uniqueid.zookeeper.ResourceTestPoolHelper.prepareClusterID;
import static org.lable.util.uniqueid.zookeeper.ResourceTestPoolHelper.prepareEmptyQueueAndPool;

public class ResourceClaimIT {

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    @Before
    public void before() throws Exception {
        ZooKeeperConnection.configure(zkInstance.getQuorumAddresses());
        ZooKeeper zookeeper = zkInstance.getZookeeperConnection();
        prepareClusterID(zookeeper, 3);
        prepareEmptyQueueAndPool(zookeeper);
        ZooKeeperConnection.reset();
    }

    @Test
    public void claimTest() throws IOException {
        ResourceClaim claim = ResourceClaim.claim(ZooKeeperConnection.get(), 2);
        int resource = claim.get();
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(2))));
    }

    @Test
    @Ignore
    public void testAgainstRealQuorum() throws IOException {
        ZooKeeperConnection.configure("zka,zkb,zkc");
        claimTest();
    }
}
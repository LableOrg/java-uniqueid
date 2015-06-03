package org.lable.oss.uniqueid.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.junit.Assert.assertThat;

public class ExpiringResourceClaimIT {

    String zookeeperQuorum;
    String znode = "/unique-id-generator";

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void before() throws IOException, KeeperException, InterruptedException {
        ZooKeeperConnection.configure(zkInstance.getQuorumAddresses());
        ZooKeeper zookeeper = ZooKeeperConnection.get();
        ResourceTestPoolHelper.prepareEmptyQueueAndPool(zookeeper, znode);
        ResourceTestPoolHelper.prepareClusterID(zookeeper, znode, 0);
        ZooKeeperConnection.reset();
    }

    @Test
    public void expirationTest() throws IOException, InterruptedException {
        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                ZooKeeperConnection.get(), 64, znode, TimeUnit.SECONDS.toMillis(2));
        int resource = claim.get();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(64))));

        // Wait for the resource to expire.
        TimeUnit.SECONDS.sleep(4);

        assertThat(claim.state, is(ResourceClaim.State.CLAIM_RELINQUISHED));
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Resource claim not held.");
        claim.get();
    }


}
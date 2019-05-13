/*
 * Copyright (C) 2014 Lable (info@lable.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lable.oss.uniqueid.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.lable.oss.dynamicconfig.zookeeper.MonitoringZookeeperConnection;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.junit.Assert.assertThat;

public class ExpiringResourceClaimIT {

    static String znode = "/unique-id-generator";

    @ClassRule
    public static ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    static MonitoringZookeeperConnection zookeeperConnection;

    @BeforeClass
    public static void before() throws IOException, KeeperException, InterruptedException {
        zookeeperConnection = new MonitoringZookeeperConnection(zkInstance.getQuorumAddresses().split(","));
        ResourceTestPoolHelper.prepareEmptyQueueAndPool(zkInstance.getZookeeperConnection(), znode);
        ResourceTestPoolHelper.prepareClusterID(zkInstance.getZookeeperConnection(), znode, 0);
    }

    @Test
    public void expirationTest() throws IOException, InterruptedException {
        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                zookeeperConnection,
                64,
                znode,
                Duration.ofSeconds(2),
                null
        );
        int resource = claim.get();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(64))));

        // Wait for the resource to expire.
        TimeUnit.SECONDS.sleep(6);

        assertThat(claim.state, is(ResourceClaim.State.CLAIM_RELINQUISHED));
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Resource claim not held.");
        claim.get();
    }
}
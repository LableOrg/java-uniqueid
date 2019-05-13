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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.lable.oss.dynamicconfig.zookeeper.MonitoringZookeeperConnection;

import java.io.IOException;
import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.junit.Assert.assertThat;
import static org.lable.oss.uniqueid.zookeeper.ResourceTestPoolHelper.*;

public class AcquisitionTimeoutIT {
    String zookeeperQuorum;
    String znode = "/unique-id-generator";

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    MonitoringZookeeperConnection zookeeperConnection;

    @Before
    public void before() throws Exception {
        zookeeperQuorum = zkInstance.getQuorumAddresses();
        zookeeperConnection = new MonitoringZookeeperConnection(zkInstance.getQuorumAddresses());
        prepareClusterID(zookeeperConnection.getActiveConnection(), znode, 3);
        prepareEmptyQueueAndPool(zookeeperConnection.getActiveConnection(), znode);
    }

    @After
    public void after() throws KeeperException, InterruptedException {
        deleteLockingTicket(zookeeperConnection.getActiveConnection(), znode);
    }

    @Test
    public void timeoutTest() throws KeeperException, InterruptedException, IOException {
        // Make the process wait indefinitely by blocking the locking ticket.
        claimLockingTicket(zookeeperConnection.getActiveConnection(), znode);

        thrown.expect(IOException.class);
        thrown.expectMessage("Process timed out.");

        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                zookeeperConnection,
                64,
                znode,
                Duration.ofSeconds(2),
                Duration.ofSeconds(2)
        );

        claim.get();
    }

    @Test
    public void timeoutTestNull() throws KeeperException, InterruptedException, IOException {
        claimLockingTicket(zookeeperConnection.getActiveConnection(), znode);

        thrown = ExpectedException.none();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("TIMER");
                try {
                    deleteLockingTicket(zookeeperConnection.getActiveConnection(), znode);
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, 2000);

        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                zookeeperConnection,
                64,
                znode,
                Duration.ofSeconds(2),
                Duration.ofSeconds(5)
        );

        int resource = claim.get();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(64))));
    }
}

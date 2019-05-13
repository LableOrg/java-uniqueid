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

import org.junit.Ignore;
import org.junit.Test;
import org.lable.oss.dynamicconfig.zookeeper.MonitoringZookeeperConnection;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.junit.Assert.assertThat;

/**
 * This class is designed to demonstrate automatic reconnection to the ZooKeeper quorum in case of connection loss.
 * To use, run it, and disable the computer's network connection for a while. Re-enable the network connection, and
 * see the connection being resumed and the test completed.
 */
public class ReconnectionIT {
    @Test
    @Ignore
    public void reconnectionTest() throws IOException, InterruptedException {
        MonitoringZookeeperConnection connection = new MonitoringZookeeperConnection("tzka,tzkb,tzkc");

        Supplier<ResourceClaim> newClaim = () -> {
            try {
                return ExpiringResourceClaim.claimExpiring(
                        connection,
                        64,
                        "/uniqueid",
                        Duration.ofSeconds(10),
                        null
                );
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };


        ResourceClaim claim = newClaim.get();
        int resource = claim.get();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(64))));

        for (int i = 0; i < 100; i++) {
            TimeUnit.SECONDS.sleep(1L);
            try {
                resource = claim.get();
            } catch (IllegalStateException e) {
                claim = newClaim.get();
                System.out.println("Expired: new claim.");
                resource = claim.get();
            }

            System.out.println(" -> " + resource);
        }
    }
}

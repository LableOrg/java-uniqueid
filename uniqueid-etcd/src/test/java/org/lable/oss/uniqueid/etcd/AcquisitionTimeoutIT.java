/*
 * Copyright © 2014 Lable (info@lable.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lable.oss.uniqueid.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.CloseableClient;
import io.etcd.jetcd.launcher.junit4.EtcdClusterResource;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lable.oss.uniqueid.etcd.ResourceClaim.LOCK_NAME;

public class AcquisitionTimeoutIT {
    @ClassRule
    public static final EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    static Client client;

    CloseableClient leaseKeepAlive;
    ByteSequence lockKey;

    @BeforeClass
    public static void setup() throws InterruptedException, ExecutionException {
        client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ByteSequence.from("unique-id/", StandardCharsets.UTF_8))
                .build();

        TestHelper.prepareClusterID(client, 5);
    }

    @Before
    public void before() throws ExecutionException, InterruptedException {
        long leaseId = client.getLeaseClient().grant(10).get().getID();

        leaseKeepAlive = EtcdHelper.keepLeaseAlive(client, leaseId, null);
        lockKey = client.getLockClient().lock(LOCK_NAME, leaseId).get().getKey();
    }

    @After
    public void after() throws InterruptedException, ExecutionException {
        client.getLockClient().unlock(lockKey).get();
        leaseKeepAlive.close();
    }

    @Test
    public void timeoutTest() throws IOException {
        thrown.expect(IOException.class);
        thrown.expectMessage("Process timed out.");

        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                client,
                64,
                Collections.singletonList(5),
                Duration.ofSeconds(2),
                Duration.ofSeconds(2)
        );

        claim.getGeneratorId();
        claim.close();
    }

    @Test
    public void timeoutTestNull() throws IOException {
        thrown = ExpectedException.none();

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("TIMER");
                try {
                    client.getLockClient().unlock(lockKey).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }, 2000);

        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                client,
                64,
                Collections.singletonList(5),
                Duration.ofSeconds(2),
                Duration.ofSeconds(5)
        );

        int resource = claim.getGeneratorId();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(64))));

        claim.close();
    }
}

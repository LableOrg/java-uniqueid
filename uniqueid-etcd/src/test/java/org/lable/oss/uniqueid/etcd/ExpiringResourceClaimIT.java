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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.lable.oss.uniqueid.etcd.ResourceClaim.POOL_PREFIX;
import static org.lable.oss.uniqueid.etcd.ResourceClaim.resourceKey;

public class ExpiringResourceClaimIT {
    @Rule
    public final EtcdTestCluster etcd = new EtcdTestCluster("test-etcd", 1);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    Client client;

    @Before
    public void before() throws IOException, InterruptedException, ExecutionException {
        client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ByteSequence.from("unique-id", StandardCharsets.UTF_8))
                .build();

        TestHelper.prepareClusterID(client, 5);
    }

    @Test
    public void expirationTest() throws IOException, InterruptedException {
        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                client,
                64,
                Collections.singletonList(5),
                Duration.ofSeconds(4),
                null
        );
        int resource = claim.getGeneratorId();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(64))));

        TimeUnit.SECONDS.sleep(2);

        int resource2 = claim.getGeneratorId();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(resource2));

        // Wait for the resource to expire.
        TimeUnit.SECONDS.sleep(4);

        assertThat(claim.state, is(ResourceClaim.State.CLAIM_RELINQUISHED));
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Resource claim not held.");
        claim.getGeneratorId();
    }

    @Test
    public void deletionTest() throws IOException, InterruptedException, ExecutionException {
        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                client,
                64,
                Collections.singletonList(5),
                // Very long expiration that shouldn't interfere with this test.
                Duration.ofSeconds(20),
                null
        );
        int resource = claim.getGeneratorId();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(64))));

        // Remove resource manually. Claim should get relinquished via watcher.
        EtcdHelper.delete(client, resourceKey(claim.getClusterId(), claim.getGeneratorId()));

        TimeUnit.MILLISECONDS.sleep(500);

        assertThat(claim.state, is(ResourceClaim.State.CLAIM_RELINQUISHED));
        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Resource claim not held.");
        claim.getGeneratorId();
    }

    @Test
    public void resourceRemovedTest() throws IOException, InterruptedException, ExecutionException {
        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                client,
                64,
                Collections.singletonList(5),
                // Very long expiration that shouldn't interfere with this test.
                Duration.ofSeconds(20),
                null
        );
        int resource = claim.getGeneratorId();
        assertThat(claim.state, is(ResourceClaim.State.HAS_CLAIM));
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(64))));

        claim.close();

        assertThat(EtcdHelper.getInt(client, POOL_PREFIX + resource), isEmpty());

        thrown.expect(IllegalStateException.class);
        thrown.expectMessage("Resource claim not held.");
        claim.getGeneratorId();
    }
}
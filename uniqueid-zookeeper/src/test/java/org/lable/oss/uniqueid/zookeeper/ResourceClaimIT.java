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


import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnection;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.CombinableMatcher.both;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.lable.oss.uniqueid.zookeeper.ResourceTestPoolHelper.prepareClusterID;
import static org.lable.oss.uniqueid.zookeeper.ResourceTestPoolHelper.prepareEmptyQueueAndPool;

public class ResourceClaimIT {

    String zookeeperQuorum;
    String znode = "/unique-id-generator";

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    ZooKeeperConnection zookeeperConnection;

    @Before
    public void before() throws Exception {
        zookeeperQuorum = zkInstance.getQuorumAddresses();
        zookeeperConnection = new ZooKeeperConnection(zkInstance.getZookeeperConnection());
        prepareClusterID(zookeeperConnection.get(), znode, 3);
        prepareEmptyQueueAndPool(zookeeperConnection.get(), znode);
    }

    @Test
    public void claimTest() throws Exception {
        ResourceClaim claim = ResourceClaim.claim(zookeeperConnection, 2, znode);
        int resource = claim.get();
        assertThat(resource, is(both(greaterThanOrEqualTo(0)).and(lessThan(2))));
    }

    @Test
    public void concurrencyTest() throws Exception {
        final int threadCount = 20;
        final int poolSize = 64;

        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ConcurrentMap<Integer, Integer> result = new ConcurrentHashMap<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final Integer number = 10 + i;
            new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    ResourceClaim claim = ResourceClaim.claim(zookeeperConnection, poolSize, znode);
                    result.put(number, claim.get());
                } catch (IOException | InterruptedException e) {
                    fail();
                }
                done.countDown();
            }, String.valueOf(number)).start();
        }

        ready.await();
        start.countDown();
        done.await();

        assertThat(result.size(), is(threadCount));

        Set<Integer> allResources = new HashSet<>();
        allResources.addAll(result.values());
        assertThat(allResources.size(), is(threadCount));
    }

    @Test
    public void concurrencyLimitedPoolTest() throws Exception {
        final int threadCount = 20;
        final int poolSize = 1;

        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ConcurrentMap<Integer, Integer> result = new ConcurrentHashMap<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final Integer number = 10 + i;
            new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    ResourceClaim claim = ResourceClaim.claim(zookeeperConnection, poolSize, znode);
                    result.put(number, claim.get());
                    claim.close();
                } catch (IOException | InterruptedException e) {
                    fail();
                }
                done.countDown();
            }, String.valueOf(number)).start();
        }

        ready.await();
        start.countDown();
        done.await();

        assertThat(result.size(), is(threadCount));

        Set<Integer> allResources = new HashSet<>();
        allResources.addAll(result.values());
        assertThat(allResources.size(), is(1));
    }
}
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

import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.IDGenerator;
import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnection;

import java.io.IOException;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class MultipleGeneratorsIT {

    String zookeeperQuorum;
    String znodeA = "/unique-id-generator-a";
    String znodeB = "/unique-id-generator-b";

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    final static int CLUSTER_ID = 4;

    @Before
    public void before() throws Exception {
        zookeeperQuorum = zkInstance.getQuorumAddresses();
        ZooKeeperConnection.configure(zookeeperQuorum);
        ZooKeeperConnection.reset();
        ZooKeeper zookeeper = ZooKeeperConnection.get();

        ResourceTestPoolHelper.prepareEmptyQueueAndPool(zookeeper, znodeA);
        ResourceTestPoolHelper.prepareClusterID(zookeeper, znodeA, CLUSTER_ID);

        ResourceTestPoolHelper.prepareEmptyQueueAndPool(zookeeper, znodeB);
        ResourceTestPoolHelper.prepareClusterID(zookeeper, znodeB, CLUSTER_ID);
    }

    @Test
    public void doubleConcurrentTest() throws Exception {
        final int threadCount = 20;
        final int batchSize = 500;

        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ConcurrentMap<Integer, Deque<byte[]>> result = new ConcurrentHashMap<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final Integer number = 10 + i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ready.countDown();
                    try {
                        start.await();
                        String znode = number % 2 == 0 ? znodeA : znodeB;
                        IDGenerator generator = SynchronizedUniqueIDGenerator.generatorFor(zookeeperQuorum, znode);
                        result.put(number, generator.batch(batchSize));
                    } catch (IOException | InterruptedException | GeneratorException e) {
                        fail();
                    }
                    done.countDown();
                }
            }, String.valueOf(number)).start();
        }

        ready.await();
        start.countDown();
        done.await();

        assertThat(result.size(), is(threadCount));

        Set<byte[]> allAIDs = new HashSet<>();
        Set<byte[]> allBIDs = new HashSet<>();
        for (Map.Entry<Integer, Deque<byte[]>> entry : result.entrySet()) {
            Integer number = entry.getKey();
            assertThat(entry.getValue().size(), is(batchSize));
            if (number % 2 == 0) {
                allAIDs.addAll(entry.getValue());
            } else {
                allBIDs.addAll(entry.getValue());
            }
        }
        assertThat(allAIDs.size(), is(threadCount * batchSize / 2));
        assertThat(allBIDs.size(), is(threadCount * batchSize / 2));
    }
}

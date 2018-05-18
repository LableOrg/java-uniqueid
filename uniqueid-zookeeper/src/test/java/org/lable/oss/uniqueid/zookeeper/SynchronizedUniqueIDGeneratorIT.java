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

import org.apache.commons.codec.binary.Hex;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.IDGenerator;
import org.lable.oss.uniqueid.bytes.Blueprint;
import org.lable.oss.uniqueid.bytes.IDBuilder;
import org.lable.oss.uniqueid.bytes.Mode;
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
import static org.lable.oss.uniqueid.zookeeper.SynchronizedUniqueIDGeneratorFactory.generatorFor;

public class SynchronizedUniqueIDGeneratorIT {
    static String znode = "/unique-id-generator";
    static ZooKeeperConnection zooKeeperConnection;

    @ClassRule
    public static ZooKeeperInstance zkInstance = new ZooKeeperInstance();
    final static int CLUSTER_ID = 4;

    @BeforeClass
    public static void before() throws Exception {
        zooKeeperConnection = new ZooKeeperConnection(zkInstance.getQuorumAddresses());
        ResourceTestPoolHelper.prepareEmptyQueueAndPool(zooKeeperConnection.get(), znode);
        ResourceTestPoolHelper.prepareClusterID(zooKeeperConnection.get(), znode, CLUSTER_ID);
    }

    @Test
    public void simpleTest() throws Exception {
        IDGenerator generator = generatorFor(zooKeeperConnection, znode, Mode.TIME_SEQUENTIAL);
        byte[] result = generator.generate();
        Blueprint blueprint = IDBuilder.parse(result);
        assertThat(result.length, is(8));
        assertThat(blueprint.getClusterId(), is(CLUSTER_ID));
    }

    @Test
    public void timeSequentialTest() throws Exception {
        SynchronizedGeneratorIdentity generatorIdentityHolder =
                new SynchronizedGeneratorIdentity(zooKeeperConnection, znode, 0, null);
        IDGenerator generator = generatorFor(generatorIdentityHolder, Mode.TIME_SEQUENTIAL);

        Set<byte[]> ids = new HashSet<>();
        for (int i = 0; i < 100_000; i++) {
            ids.add(generator.generate());
        }

        assertThat(ids.size(), is(100_000));

        byte[] id = ids.iterator().next();

        System.out.println(Hex.encodeHex(id));
        System.out.println(IDBuilder.parseTimestamp(id));
    }

    @Test
    public void concurrentTest() throws Exception {
        final int threadCount = 20;
        final int batchSize = 500;

        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ConcurrentMap<Integer, Deque<byte[]>> result = new ConcurrentHashMap<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final Integer number = 10 + i;
            new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    IDGenerator generator = generatorFor(zooKeeperConnection, znode, Mode.SPREAD);
                    result.put(number, generator.batch(batchSize));
                } catch (IOException | InterruptedException | GeneratorException e) {
                    fail();
                }
                done.countDown();
            }, String.valueOf(number)).start();
        }

        ready.await();
        start.countDown();
        done.await();

        assertThat(result.size(), is(threadCount));

        Set<byte[]> allIDs = new HashSet<>();
        for (Map.Entry<Integer, Deque<byte[]>> entry : result.entrySet()) {
            assertThat(entry.getValue().size(), is(batchSize));
            allIDs.addAll(entry.getValue());
        }
        assertThat(allIDs.size(), is(threadCount * batchSize));
    }
}

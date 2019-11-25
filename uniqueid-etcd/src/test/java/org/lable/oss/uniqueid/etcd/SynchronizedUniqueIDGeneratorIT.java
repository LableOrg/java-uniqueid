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
package org.lable.oss.uniqueid.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.junit4.EtcdClusterResource;
import org.apache.commons.codec.binary.Hex;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.lable.oss.uniqueid.ByteArray;
import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.IDGenerator;
import org.lable.oss.uniqueid.bytes.Blueprint;
import org.lable.oss.uniqueid.bytes.IDBuilder;
import org.lable.oss.uniqueid.bytes.Mode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.lable.oss.uniqueid.etcd.SynchronizedUniqueIDGeneratorFactory.generatorFor;

public class SynchronizedUniqueIDGeneratorIT {
    @ClassRule
    public static final EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

    final static int CLUSTER_ID = 4;

    static Client client;

    @BeforeClass
    public static void setup() throws InterruptedException, ExecutionException {
        client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ByteSequence.from("unique-id/", StandardCharsets.UTF_8))
                .build();

        TestHelper.prepareClusterID(client, CLUSTER_ID);
    }

    @Test
    public void simpleTest() throws Exception {
        IDGenerator generator = generatorFor(client, Mode.TIME_SEQUENTIAL);
        byte[] result = generator.generate();
        Blueprint blueprint = IDBuilder.parse(result);
        assertThat(result.length, is(8));
        assertThat(blueprint.getClusterId(), is(CLUSTER_ID));
    }

    @Test
    public void timeSequentialTest() throws Exception {
        // Explicitly implement a clock ourselves for testing.
        AtomicLong time = new AtomicLong(1_500_000_000);
        SynchronizedGeneratorIdentity generatorIdentityHolder = new SynchronizedGeneratorIdentity(
                client,
                0,
                null,
                null
        );
        IDGenerator generator = generatorFor(
                generatorIdentityHolder,
                time::getAndIncrement,
                Mode.TIME_SEQUENTIAL
        );

        Set<ByteArray> ids = new HashSet<>();
        for (int i = 0; i < 100_000; i++) {
            ids.add(new ByteArray(generator.generate()));
        }

        assertThat(ids.size(), is(100_000));

        ByteArray id = ids.iterator().next();

        System.out.println(Hex.encodeHex(id.getValue()));
        System.out.println(IDBuilder.parseTimestamp(id.getValue()));
    }

    @Test
    public void test() {
        Set<ByteArray> s = new HashSet<>();
        s.add(new ByteArray(new byte[]{0, 1}));
        s.add(new ByteArray(new byte[]{0, 1}));
        assertThat(s.size(), is(1));

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
                    IDGenerator generator = generatorFor(client, Mode.SPREAD);
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

        Set<ByteArray> allIDs = new HashSet<>();
        for (Map.Entry<Integer, Deque<byte[]>> entry : result.entrySet()) {
            assertThat(entry.getValue().size(), is(batchSize));
            entry.getValue().forEach(value -> allIDs.add(new ByteArray(value)));
        }
        assertThat(allIDs.size(), is(threadCount * batchSize));
    }
}

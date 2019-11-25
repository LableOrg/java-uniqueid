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
import io.etcd.jetcd.CloseableClient;
import io.etcd.jetcd.launcher.junit4.EtcdClusterResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.IDGenerator;
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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.lable.oss.uniqueid.etcd.SynchronizedUniqueIDGeneratorFactory.generatorFor;

public class MultipleGeneratorsIT {
    @ClassRule
    public static final EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    static Client clientA;
    static Client clientB;

    @BeforeClass
    public static void setup() throws InterruptedException, ExecutionException {
        clientA = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ByteSequence.from("unique-id-a/", StandardCharsets.UTF_8))
                .build();
        clientB = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ByteSequence.from("unique-id-b/", StandardCharsets.UTF_8))
                .build();

        TestHelper.prepareClusterID(clientA, 4);
        TestHelper.prepareClusterID(clientB, 5);
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
            new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    Client client = number % 2 == 0 ? clientA : clientB;
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

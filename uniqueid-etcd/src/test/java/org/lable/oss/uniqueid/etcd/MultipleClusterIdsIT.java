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
import io.etcd.jetcd.launcher.junit4.EtcdClusterResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.lable.oss.uniqueid.ByteArray;
import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.IDGenerator;
import org.lable.oss.uniqueid.bytes.Blueprint;
import org.lable.oss.uniqueid.bytes.IDBuilder;
import org.lable.oss.uniqueid.bytes.Mode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.lable.oss.uniqueid.etcd.SynchronizedUniqueIDGeneratorFactory.generatorFor;

public class MultipleClusterIdsIT {
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
                .namespace(ByteSequence.from("unique-id/", StandardCharsets.UTF_8))
                .build();
        clientB = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ByteSequence.from("unique-id/", StandardCharsets.UTF_8))
                .build();

        TestHelper.prepareClusterID(clientA, 4, 5, 6);
    }

    @Test
    public void doubleConcurrentTest() throws Exception {
        final int threadCount = Blueprint.MAX_GENERATOR_ID + 2;

        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ConcurrentLinkedDeque<ByteArray> result = new ConcurrentLinkedDeque<>();

        for (int i = 0; i < threadCount; i++) {
            final int number = 10 + i;
            new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    Client client = number % 2 == 0 ? clientA : clientB;
                    IDGenerator generator = generatorFor(client, Mode.SPREAD);
                    result.add(new ByteArray(generator.generate()));
                } catch (IOException | InterruptedException | GeneratorException e) {
                    fail(e.getMessage());
                }
                done.countDown();
            }, String.valueOf(number)).start();
        }

        ready.await();
        start.countDown();
        done.await();

        assertThat(result.size(), is(threadCount));

        Map<Integer, Set<Integer>> clusterGeneratorIds = new HashMap<>();

        for (ByteArray byteArray : result) {
            Blueprint blueprint = IDBuilder.parse(byteArray.getValue());
            int clusterId = blueprint.getClusterId();
            int generatorId = blueprint.getGeneratorId();

            if (!clusterGeneratorIds.containsKey(clusterId)) clusterGeneratorIds.put(clusterId, new HashSet<>());
            clusterGeneratorIds.get(clusterId).add(generatorId);
        }

        assertThat(clusterGeneratorIds.get(4).size(), is (2048));
        assertThat(clusterGeneratorIds.get(5).size(), is (1));
    }
}

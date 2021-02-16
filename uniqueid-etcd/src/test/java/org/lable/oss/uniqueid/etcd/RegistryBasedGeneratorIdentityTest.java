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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.lable.oss.uniqueid.BaseUniqueIDGenerator;
import org.lable.oss.uniqueid.GeneratorException;
import org.lable.oss.uniqueid.IDGenerator;
import org.lable.oss.uniqueid.bytes.Mode;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.lable.oss.uniqueid.etcd.SynchronizedUniqueIDGeneratorFactory.generatorFor;

public class RegistryBasedGeneratorIdentityTest {
    @Rule
    public final EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

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
    public void simpleTest() throws IOException, GeneratorException, ExecutionException, InterruptedException {
        RegistryBasedGeneratorIdentity generatorIdentity = RegistryBasedGeneratorIdentity.basedOn(
                etcd.getClientEndpoints().stream().map(URI::toString).collect(Collectors.joining(",")),
                "unique-id",
                "Hello!"
        );

        int clusterId = generatorIdentity.getClusterId();
        int generatorId = generatorIdentity.getGeneratorId();

        Optional<String> content =  EtcdHelper.get(client, RegistryBasedResourceClaim.resourceKey(clusterId, generatorId));
        assertThat(content, hasValue("Hello!"));

        generatorIdentity.close();

        content =  EtcdHelper.get(client, RegistryBasedResourceClaim.resourceKey(clusterId, generatorId));
        assertThat(content, isEmpty());
    }

    @Test
    public void multipleTest() throws IOException, GeneratorException, ExecutionException, InterruptedException {
        final int threadCount = 4;
        final int batchSize = 500;

        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ConcurrentMap<Integer, Deque<byte[]>> result = new ConcurrentHashMap<>(threadCount);
        final Set<Integer> generatorIds = new HashSet<>();

        for (int i = 0; i < threadCount; i++) {
            final Integer number = 10 + i;
            new Thread(() -> {
                ready.countDown();
                try {
                    start.await();
                    RegistryBasedGeneratorIdentity generatorIdentity = RegistryBasedGeneratorIdentity.basedOn(
                            etcd.getClientEndpoints().stream().map(URI::toString).collect(Collectors.joining(",")),
                            "unique-id",
                            "Hello!"
                    );

                    IDGenerator generator = new BaseUniqueIDGenerator(generatorIdentity, Mode.SPREAD);
                    generatorIds.add(generatorIdentity.getGeneratorId());
                    result.put(number, generator.batch(batchSize));
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
        assertThat(generatorIds.size(), is(threadCount));

        for (Map.Entry<Integer, Deque<byte[]>> entry : result.entrySet()) {
            assertThat(entry.getValue().size(), is(batchSize));
        }
    }
}
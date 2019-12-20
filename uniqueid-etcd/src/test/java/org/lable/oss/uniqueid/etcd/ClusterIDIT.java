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
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;

public class ClusterIDIT {
    @Rule
    public final EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

    @Test
    public void defaultTest() throws IOException {
        ByteSequence ns = ByteSequence.from("unique-id/", StandardCharsets.UTF_8);

        Client client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ns)
                .build();

        List<Integer> ids = ClusterID.get(client);

        assertThat(ids, contains(0));
    }

    @Test
    public void preconfiguredTest() throws ExecutionException, InterruptedException, IOException {
        ByteSequence ns = ByteSequence.from("unique-id/", StandardCharsets.UTF_8);

        Client client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ns)
                .build();

        client.getKVClient().put(ClusterID.CLUSTER_ID_KEY, ByteSequence.from("12".getBytes())).get();

        List<Integer> ids = ClusterID.get(client);

        assertThat(ids, contains(12));
    }

    @Test
    public void preconfiguredMultipleTest() throws ExecutionException, InterruptedException, IOException {
        ByteSequence ns = ByteSequence.from("unique-id/", StandardCharsets.UTF_8);

        Client client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ns)
                .build();

        client.getKVClient().put(ClusterID.CLUSTER_ID_KEY, ByteSequence.from("12, 13".getBytes())).get();

        List<Integer> ids = ClusterID.get(client);

        assertThat(ids, contains(12, 13));
    }

    @Test(expected = IOException.class)
    public void invalidValueTest() throws ExecutionException, InterruptedException, IOException {
        ByteSequence ns = ByteSequence.from("unique-id/", StandardCharsets.UTF_8);

        Client client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ns)
                .build();

        client.getKVClient().put(ClusterID.CLUSTER_ID_KEY, ByteSequence.from("BOGUS".getBytes())).get();

        ClusterID.get(client);
    }
}
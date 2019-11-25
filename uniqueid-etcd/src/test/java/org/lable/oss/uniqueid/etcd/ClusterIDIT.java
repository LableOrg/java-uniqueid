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
import java.util.concurrent.ExecutionException;

import static org.hamcrest.CoreMatchers.is;
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

        int id = ClusterID.get(client);

        assertThat(id, is(0));
    }

    @Test
    public void preconfiguredTest() throws ExecutionException, InterruptedException, IOException {
        ByteSequence ns = ByteSequence.from("unique-id/", StandardCharsets.UTF_8);

        Client client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ns)
                .build();

        client.getKVClient().put(ClusterID.CLUSTER_ID_KEY, ByteSequence.from("12".getBytes())).get();

        int id = ClusterID.get(client);

        assertThat(id, is(12));
    }
}
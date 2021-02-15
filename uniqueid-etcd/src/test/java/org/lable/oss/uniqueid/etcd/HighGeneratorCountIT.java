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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.lable.oss.uniqueid.etcd.SynchronizedUniqueIDGeneratorFactory.generatorFor;

public class HighGeneratorCountIT {
    @ClassRule
    public static final EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

    final static int CLUSTER_ID_A = 4;
    final static int CLUSTER_ID_B = 5;

    static Client client;

    @BeforeClass
    public static void setup() throws InterruptedException, ExecutionException {
        client = Client.builder()
                .endpoints(etcd.getClientEndpoints())
                .namespace(ByteSequence.from("unique-id/", StandardCharsets.UTF_8))
                .build();

        TestHelper.prepareClusterID(client, CLUSTER_ID_A, CLUSTER_ID_B);
        for (int i = 0; i < 2047; i++) {
            EtcdHelper.put(client, "pool/4:" + i);
        }
    }

    @Test
    public void above255Test() throws Exception {
        IDGenerator generator = generatorFor(client, Mode.TIME_SEQUENTIAL);
        byte[] result = generator.generate();
        Blueprint blueprint = IDBuilder.parse(result);
        assertThat(result.length, is(8));
        assertThat(blueprint.getClusterId(), is(CLUSTER_ID_A));
        assertThat(blueprint.getGeneratorId(), is(2047));

        IDGenerator generator2 = generatorFor(client, Mode.TIME_SEQUENTIAL);
        result = generator2.generate();
        blueprint = IDBuilder.parse(result);
        assertThat(result.length, is(8));
        assertThat(blueprint.getClusterId(), is(CLUSTER_ID_B));
        assertThat(blueprint.getGeneratorId(), is(0));
    }
}

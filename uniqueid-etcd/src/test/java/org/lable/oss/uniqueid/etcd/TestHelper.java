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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.lable.oss.uniqueid.etcd.ClusterID.CLUSTER_ID_KEY;

public class TestHelper {

    public static void prepareClusterID(Client etcd, int... clusterId) throws ExecutionException, InterruptedException {
        String serialized = Arrays.stream(clusterId).boxed().map(String::valueOf).collect(Collectors.joining(", "));

        etcd.getKVClient()
                .put(CLUSTER_ID_KEY, ByteSequence.from(serialized, StandardCharsets.UTF_8))
                .get();
    }
}

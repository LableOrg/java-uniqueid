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
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class ClusterID {
    final static ByteSequence CLUSTER_ID_KEY = ByteSequence.from("cluster-id", StandardCharsets.UTF_8);
    final static int DEFAULT_CLUSTER_ID = 0;

    /**
     * Retrieves the numeric cluster ID from the Etcd cluster.
     *
     * @param etcd Etcd connection.
     * @return The cluster ID, if configured in the cluster.
     * @throws IOException Thrown when retrieving the ID fails.
     */
    public static int get(Client etcd) throws IOException {
        GetResponse get;
        try {
            get = etcd.getKVClient().get(CLUSTER_ID_KEY).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }

        Integer id = null;

        for (KeyValue kv : get.getKvs()) {
            if (kv.getKey().equals(CLUSTER_ID_KEY)) {
                String value = kv.getValue().toString(StandardCharsets.UTF_8);
                id = Integer.parseInt(value);
                break;
            }
        }

        if (id == null) {
            ByteSequence defaultValue = ByteSequence.from(String.valueOf(DEFAULT_CLUSTER_ID).getBytes());
            try {
                etcd.getKVClient().put(CLUSTER_ID_KEY, defaultValue).get();
                return DEFAULT_CLUSTER_ID;
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        } else {
            return id;
        }
    }
}

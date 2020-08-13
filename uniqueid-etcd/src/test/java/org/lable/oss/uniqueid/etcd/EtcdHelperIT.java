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

import io.etcd.jetcd.Client;
import io.etcd.jetcd.launcher.junit4.EtcdClusterResource;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.lable.oss.uniqueid.etcd.EtcdHelper.*;

public class EtcdHelperIT {
    @Rule
    public final EtcdClusterResource etcd = new EtcdClusterResource("test-etcd", 1);

    @Test
    public void test() throws ExecutionException, InterruptedException {
        Client client = Client.builder().endpoints(etcd.getClientEndpoints()).build();

        assertThat(getInt(client, "a"), isEmpty());

        put(client, "a", -2);

        assertThat(getInt(client, "a"), hasValue(-2));

        delete(client, "a");

        assertThat(getInt(client, "a"), isEmpty());
    }
}
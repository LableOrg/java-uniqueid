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

/*
 * Copyright © 2015 Lable (info@lable.nl)
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

import io.etcd.jetcd.launcher.Etcd;
import io.etcd.jetcd.launcher.EtcdCluster;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.net.URI;
import java.util.List;

/**
 * The jetcd library dropped support for JUnit4, so we wrap the cluster ourselves.
 */
public class EtcdTestCluster implements TestRule {

    private final String clusterName;
    private final int nodes;
    private final boolean ssl;
    private EtcdCluster cluster;

    public EtcdTestCluster(String clusterName, int nodes) {
        this(clusterName, nodes, false);
    }

    public EtcdTestCluster(String clusterName, int nodes, boolean ssl) {
        this.clusterName = clusterName;
        this.nodes = nodes;
        this.ssl = ssl;
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                cluster = Etcd.builder()
                        .withClusterName(clusterName)
                        .withNodes(nodes)
                        .withSsl(ssl)
                        .build();

                cluster.start();
                try {
                    base.evaluate();
                } finally {
                    cluster.close();
                    cluster = null;
                }
            }
        };
    }

    public List<URI> getClientEndpoints() {
        return cluster.clientEndpoints();
    }
}
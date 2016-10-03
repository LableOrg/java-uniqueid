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
package org.lable.oss.uniqueid.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Rule;
import org.junit.Test;
import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnection;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ClusterIDIT {

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    final static int CLUSTER_ID = 15;

    @Test
    public void getClusterIDTest() throws Exception {
        ZooKeeper zookeeper = zkInstance.getZookeeperConnection();
        ResourceTestPoolHelper.prepareClusterID(zookeeper, "/some-path", CLUSTER_ID);

        int id = ClusterID.get(zookeeper, "/some-path");
        assertThat(id, is(CLUSTER_ID));

        ZooKeeperConnection.reset();
    }

    @Test
    public void getClusterIDDefaultTest() throws Exception {
        ZooKeeper zookeeper = zkInstance.getZookeeperConnection();

        int id = ClusterID.get(zookeeper, "/some-path");
        assertThat(id, is(ClusterID.DEFAULT_CLUSTER_ID));

        ZooKeeperConnection.reset();
    }
}
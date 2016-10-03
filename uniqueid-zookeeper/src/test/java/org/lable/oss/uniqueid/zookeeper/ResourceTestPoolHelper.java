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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * Static methods for filling a ZooKeeper testing tree.
 */
public class ResourceTestPoolHelper {

    /**
     * Create the two znodes used for the queue and the resource pool.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void prepareEmptyQueueAndPool(ZooKeeper zookeeper, String znode)
            throws KeeperException, InterruptedException {
        try {
            zookeeper.create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }
        zookeeper.create(znode + "/queue", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zookeeper.create(znode + "/pool", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * Create the cluster ID znode.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param clusterId Cluster ID to configure.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void prepareClusterID(ZooKeeper zookeeper, String znode, int clusterId)
            throws KeeperException, InterruptedException {
        try {
            zookeeper.create(znode, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            if (e.code() != KeeperException.Code.NODEEXISTS) {
                throw e;
            }
        }
        zookeeper.create(znode + "/cluster-id", String.valueOf(clusterId).getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
}

/**
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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

import static org.lable.oss.uniqueid.zookeeper.ZooKeeperHelper.create;
import static org.lable.oss.uniqueid.zookeeper.ZooKeeperHelper.mkdirp;

/**
 * Retrieves the numeric cluster ID from the ZooKeeper quorum.
 */
public class ClusterID {
    final static String CLUSTER_ID_NODE = "/cluster-id";
    final static int DEFAULT_CLUSTER_ID = 0;

    /**
     * Retrieves the numeric cluster ID from the ZooKeeper quorum.
     *
     * @param zookeeper ZooKeeper instance to work with.
     * @return The cluster ID, if configured in the quorum.
     * @throws IOException                     Thrown when retrieving the ID fails.
     * @throws java.lang.NumberFormatException Thrown when the supposed ID found in the quorum could not be parsed as
     *                                         an integer.
     */
    public static int get(ZooKeeper zookeeper, String znode) throws IOException {
        try {
            Stat stat = zookeeper.exists(znode + CLUSTER_ID_NODE, false);
            if (stat == null) {
                mkdirp(zookeeper, znode);
                create(zookeeper, znode + CLUSTER_ID_NODE, String.valueOf(DEFAULT_CLUSTER_ID).getBytes());
            }

            byte[] id = zookeeper.getData(znode + CLUSTER_ID_NODE, false, null);
            return Integer.valueOf(new String(id));
        } catch (KeeperException e) {
            throw new IOException(String.format("Failed to retrieve the cluster ID from the ZooKeeper quorum. " +
                    "Expected to find it at znode %s.", znode + CLUSTER_ID_NODE), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }
}

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

import org.lable.oss.uniqueid.zookeeper.connection.ZooKeeperConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * {@link ResourceClaim} that automatically relinquishes its hold on a resource
 * after a set amount of time.
 */
public class ExpiringResourceClaim extends ResourceClaim {
    private static final Logger logger = LoggerFactory.getLogger(ExpiringResourceClaim.class);

    public final static long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    ExpiringResourceClaim(ZooKeeperConnection zooKeeperConnection, int poolSize, String znode, long timeout) throws IOException {
        super(zooKeeperConnection, poolSize, znode);
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                close();
            }
        }, timeout);
    }

    /**
     * Claim a resource.
     *
     * @param zooKeeperConnection ZooKeeper connection to use.
     * @param poolSize            Size of the resource pool.
     * @param znode               Root znode of the ZooKeeper resource-pool.
     * @return A resource claim.
     */
    public static ResourceClaim claimExpiring(ZooKeeperConnection zooKeeperConnection, int poolSize, String znode)
            throws IOException {
        return claimExpiring(zooKeeperConnection, poolSize, znode, DEFAULT_TIMEOUT);
    }

    /**
     * Claim a resource.
     *
     * @param zooKeeperConnection ZooKeeper connection to use.
     * @param poolSize            Size of the resource pool.
     * @param znode               Root znode of the ZooKeeper resource-pool.
     * @param timeout             Delay in milliseconds before the claim expires.
     * @return A resource claim.
     */
    public static ResourceClaim claimExpiring(ZooKeeperConnection zooKeeperConnection,
                                              int poolSize,
                                              String znode,
                                              Long timeout)
            throws IOException {

        long timeoutNonNull = timeout == null ? DEFAULT_TIMEOUT : timeout;
        logger.debug("Preparing expiring resource-claim; will release it in {}ms.", timeout);

        return new ExpiringResourceClaim(zooKeeperConnection, poolSize, znode, timeoutNonNull);
    }
}

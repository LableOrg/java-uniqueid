package org.lable.oss.uniqueid.zookeeper;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * {@link ResourceClaim} that automatically relinquishes its hold on a resource
 * after a set amount of time.
 */
public class ExpiringResourceClaim extends ResourceClaim {

    public final static long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    ExpiringResourceClaim(ZooKeeper zookeeper, int poolSize, String znode, long timeout) throws IOException {
        super(zookeeper, poolSize, znode);
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                close();
            }
        }, timeout);
    }

    /**
     * Claim a resource.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param poolSize Size of the resource pool.
     * @param znode Root znode of the ZooKeeper resource-pool.
     * @return A resource claim.
     * @throws IOException
     */
    public static ResourceClaim claimExpiring(ZooKeeper zookeeper, int poolSize, String znode)
            throws IOException {
        return claimExpiring(zookeeper, poolSize, znode, DEFAULT_TIMEOUT);
    }

    /**
     * Claim a resource.
     *
     * @param zookeeper ZooKeeper connection to use.
     * @param poolSize Size of the resource pool.
     * @param znode Root znode of the ZooKeeper resource-pool.
     * @param timeout Delay in milliseconds before the claim expires.
     * @return A resource claim.
     * @throws IOException
     */
    public static ResourceClaim claimExpiring(ZooKeeper zookeeper, int poolSize, String znode, long timeout)
            throws IOException {
        return new ExpiringResourceClaim(zookeeper, poolSize, znode, timeout);
    }
}

package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

/**
 * {@link org.lable.util.uniqueid.zookeeper.ResourceClaim} that automatically relinquishes its hold on a resource
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

    public static ResourceClaim claim(ZooKeeper zookeeper, String znode, int poolSize) throws IOException {
        return claim(zookeeper, poolSize, znode, DEFAULT_TIMEOUT);
    }

    public static ResourceClaim claim(ZooKeeper zookeeper, int poolSize, String znode, long timeout)
            throws IOException {
        return new ExpiringResourceClaim(zookeeper, poolSize, znode, timeout);
    }
}

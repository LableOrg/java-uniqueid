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

    public final static long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(5);

    ExpiringResourceClaim(ZooKeeper zookeeper, int poolSize, long timeout) throws IOException {
        super(zookeeper, poolSize);
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run(){
                close();
            }
        }, timeout);
    }

    public static ResourceClaim claim(int poolSize) throws IOException {
        return claim(poolSize, DEFAULT_TIMEOUT);
    }

    public static ResourceClaim claim(int poolSize, long timeout) throws IOException {
        return new ExpiringResourceClaim(ZooKeeperConnection.get(), poolSize, timeout);
    }
}

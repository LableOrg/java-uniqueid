package org.lable.oss.uniqueid.zookeeper;

import org.junit.Test;
import org.lable.oss.dynamicconfig.zookeeper.MonitoringZookeeperConnection;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TempIT {
    @Test
    public void test() throws IOException, InterruptedException {
        MonitoringZookeeperConnection zookeeperConnection = new MonitoringZookeeperConnection("tzka,tzkb,tzkc");

        for (int i = 0; i < 10; i++) {
            ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                    zookeeperConnection,
                    256,
                    "/unique-id-generator",
                    TimeUnit.SECONDS.toMillis(30)
            );
            System.out.println(claim.get());
        }


        TimeUnit.SECONDS.sleep(35);
    }

    @Test
    public void test2() throws IOException, InterruptedException {
        MonitoringZookeeperConnection zookeeperConnection = new MonitoringZookeeperConnection("tzka,tzkb,tzkc");

        ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                zookeeperConnection,
                256,
                "/unique-id-generator",
                TimeUnit.SECONDS.toMillis(30)
        );
        System.out.println(claim.get());


        TimeUnit.SECONDS.sleep(35);
    }
}

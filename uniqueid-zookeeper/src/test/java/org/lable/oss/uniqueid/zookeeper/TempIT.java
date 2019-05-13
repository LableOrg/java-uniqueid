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

import org.junit.Ignore;
import org.junit.Test;
import org.lable.oss.dynamicconfig.zookeeper.MonitoringZookeeperConnection;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TempIT {
    @Test
    @Ignore
    public void test() throws IOException, InterruptedException, ExecutionException {
        MonitoringZookeeperConnection zookeeperConnection = new MonitoringZookeeperConnection("tzka,tzkb,tzkc");

        final int POOL_SIZE = 10;

        ExecutorService pool = Executors.newFixedThreadPool(POOL_SIZE);
        CompletionService<Integer> completionService = new ExecutorCompletionService<Integer>(pool);

        for (int i = 0; i < POOL_SIZE; i++) {
            completionService.submit(() -> {
                try {
                    ResourceClaim claim = ExpiringResourceClaim.claimExpiring(
                            zookeeperConnection,
                            256,
                            "/unique-id-generator",
                            TimeUnit.SECONDS.toMillis(30)
                    );
                    return claim.get();
                } catch (IOException e) {
                    return null;
                }
            });
        }

        Set<Integer> ids = new HashSet<>();
        for (int i = 0; i < POOL_SIZE; i++) {
            Future<Integer> future = completionService.take();
            ids.add(future.get());
        }

        TimeUnit.SECONDS.sleep(35);

        assertThat(ids.size(), is(POOL_SIZE));
    }

    @Test
    @Ignore
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

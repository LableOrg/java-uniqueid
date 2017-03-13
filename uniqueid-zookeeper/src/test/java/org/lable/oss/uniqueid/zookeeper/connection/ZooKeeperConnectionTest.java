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
package org.lable.oss.uniqueid.zookeeper.connection;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.lable.oss.uniqueid.zookeeper.ZooKeeperInstance;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ZooKeeperConnectionTest {

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    @Rule
    public ExpectedException exceptionGrabber = ExpectedException.none();

    @Test
    public void test() throws Throwable {
        ZooKeeperConnection zooKeeperConnection = new ZooKeeperConnection("localhost:21818");
        ZooKeeper zooKeeper = zooKeeperConnection.getActiveConnection();

        assertThat(zooKeeper.getState(), is(ZooKeeper.States.CONNECTED));

        // Stop the local ZooKeeper. This way we force a lost connection.
        zkInstance.after();
        TimeUnit.SECONDS.sleep(5);

        // Assert that we cannot reconnect to the quorum, and verify that we don't get stuck in an infinite loop
        // reconnecting.
        exceptionGrabber.expect(IOException.class);
        zooKeeperConnection.getActiveConnection();

        // Try to reconnect to a new quorum instance.
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread(() -> {
            try {
                latch.countDown();
                latch.await(10, TimeUnit.SECONDS);

                TimeUnit.SECONDS.sleep(7);

                // Start a new instance.
                zkInstance.before();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable throwable) {
                fail();
            }
        }).start();

        latch.countDown();
        latch.await(10, TimeUnit.SECONDS);

        // Start requesting a live connection before the ZooKeeper instance is up again, to test the reconnection loop.
        zooKeeper = zooKeeperConnection.getActiveConnection();
        assertThat(zooKeeper.getState(), is(ZooKeeper.States.CONNECTED));
    }
}
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


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Singleton for a ZooKeeper connection object instance.
 */
public class ZooKeeperConnection {

    final static Logger logger = LoggerFactory.getLogger(ZooKeeperConnection.class);

    /**
     * Connection timeout, in seconds.
     */
    final static int CONNECTION_TIMEOUT = 10;

    final Queue<ZooKeeperConnectionObserver> observers = new ConcurrentLinkedQueue<>();
    final String quorumAddresses;

    ZooKeeper zookeeper = null;

    public ZooKeeperConnection(String quorumAddresses) throws IOException {
        this.zookeeper = connect(quorumAddresses);
        this.quorumAddresses = quorumAddresses;
        zookeeper.register(new ConnectionWatcher(this));
    }

    public ZooKeeper get() throws IOException {
        if (zookeeper == null) {
            zookeeper = connect(quorumAddresses);
        }
        return zookeeper;
    }

    public void shutdown() {
        if (zookeeper == null) return;

        try {
            zookeeper.close();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            zookeeper = null;
        }
    }

    /**
     * Connect to the ZooKeeper quorum, or timeout if it is unreachable.
     *
     * @throws IOException Thrown when connecting to the ZooKeeper quorum fails.
     */
    private ZooKeeper connect(String quorumAddresses) throws IOException {
        final CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper zookeeper;

        // Connect to the quorum and wait for the successful connection callback.;
        zookeeper = new ZooKeeper(quorumAddresses, (int) SECONDS.toMillis(10), watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                // Signal that the Zookeeper connection is established.
                latch.countDown();
            }
        });

        boolean successfullyConnected = false;
        try {
            successfullyConnected = latch.await(11, SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!successfullyConnected) {
            throw new IOException(String.format(
                    "Connection to ZooKeeper quorum timed out after %d seconds.", CONNECTION_TIMEOUT));
        }

        return zookeeper;
    }

    public void registerObserver(ZooKeeperConnectionObserver observer) {
        observers.add(observer);
    }

    public void deregisterObserver(ZooKeeperConnectionObserver observer) {
        observers.remove(observer);
    }

    /**
     * Remove any preexisting ZooKeeper connection instance.
     * <p>
     * This method should be called when the connection to the ZooKeeper is expired, so a subsequent call to
     * {@link #get()} will establish a new connection.
     */
    public void reset() {
        zookeeper = null;
    }

    static class ConnectionWatcher implements Watcher {

        private final ZooKeeperConnection zooKeeperConnection;

        public ConnectionWatcher(ZooKeeperConnection zooKeeperConnection) {
            this.zooKeeperConnection = zooKeeperConnection;
        }

        @Override
        public void process(WatchedEvent event) {
            switch (event.getState()) {
                case Disconnected:
                    logger.warn("Disconnected from ZooKeeper quorum.");
                    zooKeeperConnection.observers.forEach(ZooKeeperConnectionObserver::disconnected);
                    break;
                case Expired:
                    zooKeeperConnection.reset();
                    break;
                case SyncConnected:
                    zooKeeperConnection.observers.forEach(ZooKeeperConnectionObserver::connected);
                    break;
                case AuthFailed:
                case ConnectedReadOnly:
                case SaslAuthenticated:
                    break;
            }
        }
    }
}

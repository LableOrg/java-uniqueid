package org.lable.util.uniqueid.zookeeper;

import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.junit.rules.ExternalResource;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Testing rule that launches a local ZooKeeper server to test against.
 *
 * @see #getZookeeperConnection()
 */
public class ZooKeeperInstance extends ExternalResource {

    private final static String DEFAULT_PORT = "21818";

    private File tempFolder;
    private ZooKeeperThread zkThread;
    private ZooKeeper zookeeper;

    private final String serverPort;

    /**
     * Create a ZooKeeperInstance that launches a local ZooKeeper server with the default port.
     */
    public ZooKeeperInstance() {
        this(DEFAULT_PORT);
    }

    /**
     * Create a ZooKeeperInstance that launches a local ZooKeeper server, overriding the default port.
     *
     * @param serverPort Port to use for the server.
     */
    public ZooKeeperInstance(String serverPort) {
        this.serverPort = serverPort;
    }

    @Override
    protected void before() throws Throwable {
        tempFolder = createTempDir();
        String zookeeperHost = "localhost:" + serverPort;
        ServerConfig config = new ServerConfig();
        config.parse(new String[]{serverPort, tempFolder.getAbsolutePath()});

        zkThread = new ZooKeeperThread(config);
        new Thread(zkThread).start();

        final CountDownLatch latch = new CountDownLatch(1);

        // Connect to the quorum and wait for the successful connection callback.
        zookeeper = new ZooKeeper(zookeeperHost, (int) TimeUnit.SECONDS.toMillis(10), new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    // Signal that the Zookeeper connection is established.
                    latch.countDown();
                }
            }
        });

        // Wait for the connection to be established.
        boolean successfullyConnected = latch.await(12, TimeUnit.SECONDS);
        if (!successfullyConnected) {
            tempFolder.delete();
            throw new Exception("Could not start a local ZooKeeper quorum for testing.");
        }
    }

    @Override
    protected void after() {
        zkThread.shutdown();
        removeTempDir(tempFolder);
    }

    /**
     * Return a ZooKeeper connection instance connected to the local testing ZooKeeper service launched.
     *
     * @return The ZooKeeper instance.
     */
    public ZooKeeper getZookeeperConnection() {
        return zookeeper;
    }

    /**
     * Get the "quorum" addresses of the local ZooKeeper server.
     *
     * @return The server address string expected by {@link org.apache.zookeeper.ZooKeeper}.
     */
    public String getQuorumAddresses() {
        return "localhost:" + serverPort;
    }

    private File createTempDir() throws IOException {
        File dir = File.createTempFile("zookeeper-junit-", "test");
        if (!(dir.delete() && dir.mkdir())) {
            throw new IOException("Failed to create temp directory.");
        }
        return dir;
    }

    private void removeTempDir(File dir) {
        try {
            FileUtils.deleteDirectory(dir);
        } catch (IOException e) {
            try {
                // Try one more time, the ZooKeeper server might have been slow in shutting down.
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            try {
                FileUtils.deleteDirectory(dir);
            } catch (IOException ioe) {
                System.err.println("Failed to delete temp directory at " + dir.getAbsolutePath());
            }
        }
    }

    /**
     * Wrap ZooKeeperServerMain in a thread, so we can start and stop it.
     */
    public static class ZooKeeperThread extends ZooKeeperServerMain implements Runnable {
        private final ServerConfig config;

        public ZooKeeperThread(ServerConfig config) {
            super();
            this.config = config;
        }

        public void shutdown() {
            try {
                super.shutdown();
            } catch (Throwable e) {
                // Ignore.
            }
        }

        @Override
        public void run() {
            try {
                runFromConfig(config);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

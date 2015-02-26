package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.lable.util.uniqueid.BaseUniqueIDGenerator;
import org.lable.util.uniqueid.GeneratorException;
import org.lable.util.uniqueid.IDGenerator;
import org.lable.util.uniqueid.zookeeper.connection.ZooKeeperConnection;

import java.io.IOException;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.lable.util.uniqueid.zookeeper.ResourceTestPoolHelper.prepareClusterID;
import static org.lable.util.uniqueid.zookeeper.ResourceTestPoolHelper.prepareEmptyQueueAndPool;

public class MultipleGeneratorsIT {

    String zookeeperQuorum;
    String znodeA = "/unique-id-generator-a";
    String znodeB = "/unique-id-generator-b";

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    final static int CLUSTER_ID = 4;

    @Before
    public void before() throws Exception {
        zookeeperQuorum = zkInstance.getQuorumAddresses();
        ZooKeeperConnection.configure(zookeeperQuorum);
        ZooKeeperConnection.reset();
        ZooKeeper zookeeper = ZooKeeperConnection.get();

        prepareEmptyQueueAndPool(zookeeper, znodeA);
        prepareClusterID(zookeeper, znodeA, CLUSTER_ID);

        prepareEmptyQueueAndPool(zookeeper, znodeB);
        prepareClusterID(zookeeper, znodeB, CLUSTER_ID);
    }

    @Test
    public void doubleConcurrentTest() throws Exception {
        final int threadCount = 20;
        final int batchSize = 500;

        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ConcurrentMap<Integer, Deque<byte[]>> result = new ConcurrentHashMap<>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final Integer number = 10 + i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ready.countDown();
                    try {
                        start.await();
                        String znode = number % 2 == 0 ? znodeA : znodeB;
                        IDGenerator generator = SynchronizedUniqueIDGenerator.generatorFor(zookeeperQuorum, znode);
                        result.put(number, generator.batch(batchSize));
                    } catch (IOException | InterruptedException | GeneratorException e) {
                        fail();
                    }
                    done.countDown();
                }
            }, String.valueOf(number)).start();
        }

        ready.await();
        start.countDown();
        done.await();

        assertThat(result.size(), is(threadCount));

        Set<byte[]> allAIDs = new HashSet<>();
        Set<byte[]> allBIDs = new HashSet<>();
        for (Map.Entry<Integer, Deque<byte[]>> entry : result.entrySet()) {
            Integer number = entry.getKey();
            assertThat(entry.getValue().size(), is(batchSize));
            if (number % 2 == 0) {
                allAIDs.addAll(entry.getValue());
            } else {
                allBIDs.addAll(entry.getValue());
            }
        }
        assertThat(allAIDs.size(), is(threadCount * batchSize / 2));
        assertThat(allBIDs.size(), is(threadCount * batchSize / 2));
    }
}

package org.lable.util.uniqueid.zookeeper;

import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.lable.util.uniqueid.GeneratorException;
import org.lable.util.uniqueid.UniqueIDGenerator;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.lable.util.uniqueid.zookeeper.ResourceTestPoolHelper.prepareClusterID;
import static org.lable.util.uniqueid.zookeeper.ResourceTestPoolHelper.prepareEmptyQueueAndPool;

public class ZooKeeperSynchronizedIT {

    @Rule
    public ZooKeeperInstance zkInstance = new ZooKeeperInstance();

    final static int CLUSTER_ID = 4;

    @Before
    public void before() throws Exception {
        ZooKeeperConnection.configure(zkInstance.getQuorumAddresses());
        ZooKeeperConnection.reset();
        ZooKeeper zookeeper = ZooKeeperConnection.get();
        prepareEmptyQueueAndPool(zookeeper);
        prepareClusterID(zookeeper, CLUSTER_ID);
    }

    @Test
    public void simpleTest() throws Exception {
        UniqueIDGenerator generator = SynchronizedUniqueIDGenerator.generator();
        byte[] result = generator.generate();
        UniqueIDGenerator.Blueprint blueprint = UniqueIDGenerator.parse(result);
        assertThat(result.length, is(8));
        assertThat(blueprint.getClusterId(), is(CLUSTER_ID));
    }

    @Test
    public void concurrentTest() throws Exception {
        final int threadCount = 20;
        final int batchSize = 500;

        final CountDownLatch ready = new CountDownLatch(threadCount);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threadCount);
        final ConcurrentMap<Integer, Deque<byte[]>> result = new ConcurrentHashMap<Integer, Deque<byte[]>>(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final Integer number = 10 + i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    ready.countDown();
                    try {
                        start.await();
                        UniqueIDGenerator generator = SynchronizedUniqueIDGenerator.generator();
                        result.put(number, generator.batch(batchSize));
                    } catch (IOException e) {
                        fail();
                    } catch (InterruptedException e) {
                        fail();
                    } catch (GeneratorException e) {
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

        Set<byte[]> allIDs = new HashSet<byte[]>();
        for (Map.Entry<Integer, Deque<byte[]>> entry : result.entrySet()) {
            assertThat(entry.getValue().size(), is(batchSize));
            allIDs.addAll(entry.getValue());
        }
        assertThat(allIDs.size(), is(threadCount * batchSize));
    }

    @Test
    @Ignore
    public void testAgainstRealQuorum() throws Exception {
        ZooKeeperConnection.configure("zka,zkb,zkc");
        concurrentTest();
    }
}

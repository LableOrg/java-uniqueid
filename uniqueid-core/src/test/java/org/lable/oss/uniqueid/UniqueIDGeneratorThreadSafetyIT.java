package org.lable.oss.uniqueid;

import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Test thread safety.
 */
public class UniqueIDGeneratorThreadSafetyIT {

    @Test
    public void multipleInstancesTest() throws InterruptedException {
        final Set<String> ids = Collections.synchronizedSet(new HashSet<String>());
        final int threadCount = 20;
        final int iterationCount = 10000;
        final CountDownLatch latch = new CountDownLatch(threadCount);

        // Generate IDs for the same generator-ID and cluster-ID in multiple threads.
        // Collision of IDs is almost guaranteed if the generator doesn't handle multi-threading gracefully.

        for (int i = 0; i < threadCount; i++) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    IDGenerator generator = LocalUniqueIDGenerator.generatorFor(1, 1);
                    try {
                        for (int i = 0; i < iterationCount; i++) {
                            byte[] id = generator.generate();
                            String asHex = Hex.encodeHexString(id);
                            ids.add(asHex);
                        }
                    } catch (GeneratorException e) {
                        // Test will fail due to missing IDs.
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });
            t.start();
        }

        // Wait for all the threads to finish, or timeout.
        boolean successfullyUnlatched = latch.await(20, TimeUnit.SECONDS);
        assertThat(successfullyUnlatched, is(true));

        // If the set holds fewer items than this, duplicates were generated.
        assertThat(ids.size(), is(threadCount * iterationCount));
    }

    @Test
    public void moreThanOneGeneratorClusterIDTest() throws InterruptedException {
        final Set<String> ids = Collections.synchronizedSet(new HashSet<String>());
        // {generatorId, clusterId}
        final int[][] profiles = {
                {0, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 15},
                {2, 0}, {3, 0}, {4, 0}, {5, 0}, {63, 0}
        };
        final int iterationCount = 10000;
        final CountDownLatch latch = new CountDownLatch(profiles.length);

        // Generate IDs for different generator-IDs and cluster-IDs in multiple threads.
        // Collision of IDs is almost guaranteed if the generator doesn't handle multi-threading gracefully.

        for (final int[] profile : profiles) {
            Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    IDGenerator generator = LocalUniqueIDGenerator.generatorFor(profile[0], profile[1]);
                    try {
                        for (int i = 0; i < iterationCount; i++) {
                            byte[] id = generator.generate();
                            String asHex = Hex.encodeHexString(id);
                            ids.add(asHex);
                        }
                    } catch (GeneratorException e) {
                        // Test will fail due to missing IDs.
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });
            t.start();
        }

        // Wait for all the threads to finish, or timeout.
        boolean successfullyUnlatched = latch.await(20, TimeUnit.SECONDS);
        assertThat(successfullyUnlatched, is(true));

        // If the set holds fewer items than this, duplicates were generated.
        assertThat(ids.size(), is(profiles.length * iterationCount));
    }
}

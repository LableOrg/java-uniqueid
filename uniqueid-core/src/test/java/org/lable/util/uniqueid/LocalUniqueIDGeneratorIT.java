package org.lable.util.uniqueid;


import org.junit.Test;

import java.util.Deque;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class LocalUniqueIDGeneratorIT {

    @Test
    public void batchTest() throws Exception {
        final int GENERATOR_ID = 42;
        final int CLUSTER_ID = 7;
        final int BATCH_SIZE = 500;
        UniqueIDGenerator generator = LocalUniqueIDGenerator.generatorFor(GENERATOR_ID, CLUSTER_ID);

        Deque<byte[]> stack = generator.batch(BATCH_SIZE);
        assertThat(stack.size(), is(BATCH_SIZE));

        UniqueIDGenerator.Blueprint blueprint = UniqueIDGenerator.parse(stack.pop());
        assertThat(blueprint.getGeneratorId(), is(GENERATOR_ID));
        assertThat(blueprint.getClusterId(), is(CLUSTER_ID));
    }
}
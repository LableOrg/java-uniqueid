package org.lable.util.uniqueid;

import org.junit.Test;

import java.util.Deque;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;

public class OnePerMillisecondDecoratorIT {
    @Test
    public void batchTest() throws Exception {
        final int GENERATOR_ID = 42;
        final int CLUSTER_ID = 7;
        final int BATCH_SIZE = 500;

        IDGenerator generator = LocalUniqueIDGenerator.generatorFor(GENERATOR_ID, CLUSTER_ID);
        IDGenerator decorator = new OnePerMillisecondDecorator(generator);

        Deque<byte[]> stack = decorator.batch(BATCH_SIZE);
        assertThat(stack.size(), is(BATCH_SIZE));

        byte[] first = stack.pop();
        BaseUniqueIDGenerator.Blueprint blueprint = BaseUniqueIDGenerator.parse(first);
        assertThat(blueprint.getGeneratorId(), is(GENERATOR_ID));
        assertThat(blueprint.getClusterId(), is(CLUSTER_ID));

        // Verify that subsequent IDs don't start with the same byte as their predecessors if generated with a
        // one-per-millisecond decorator wrapping the generator. It *is* possible for the bytes to be the same even
        // with this decorator, but not when they are generated as fast as possible.
        byte previous = first[0];
        for (byte[] bytes : stack) {
            byte current = bytes[0];
            assertThat(previous, is(not(current)));
            previous = current;
        }
    }
}
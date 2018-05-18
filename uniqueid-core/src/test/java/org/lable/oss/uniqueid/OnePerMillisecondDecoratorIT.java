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
package org.lable.oss.uniqueid;

import org.junit.Test;
import org.lable.oss.uniqueid.bytes.Blueprint;
import org.lable.oss.uniqueid.bytes.IDBuilder;
import org.lable.oss.uniqueid.bytes.Mode;

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

        IDGenerator generator = LocalUniqueIDGeneratorFactory.generatorFor(
                GENERATOR_ID,
                CLUSTER_ID,
                Mode.SPREAD
        );
        IDGenerator decorator = OnePerMillisecondDecorator.decorate(generator);

        Deque<byte[]> stack = decorator.batch(BATCH_SIZE);
        assertThat(stack.size(), is(BATCH_SIZE));

        byte[] first = stack.pop();
        Blueprint blueprint = IDBuilder.parse(first);
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
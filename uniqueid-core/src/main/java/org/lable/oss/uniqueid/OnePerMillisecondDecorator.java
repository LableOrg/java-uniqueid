/**
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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Decorator for an {@link IDGenerator} that sleeps at least a millisecond between each invocation to guarantee ID
 * spread.
 * </p>
 * <p>
 * This is not normally necessary nor desired, but can be useful when you want to generate several IDs, but you don't
 * want subsequent IDs to start with the same byte.
 * </p>
 * <p>
 * This class is of course significantly slower than using an undecorated generator.
 * </p>
 */
public class OnePerMillisecondDecorator implements IDGenerator {
    final IDGenerator generator;
    long previousInvocation = 0;
    byte[] previous = null;

    protected OnePerMillisecondDecorator(IDGenerator generator) {
        this.generator = generator;
    }

    /**
     * Wrap an {@link IDGenerator} in a OnePerMillisecondDecorator.
     *
     * @param generator Generator to decorate.
     * @return The decorated generator.
     */
    public static IDGenerator decorate(IDGenerator generator) {
        return new OnePerMillisecondDecorator(generator);
    }

    @Override
    public byte[] generate() throws GeneratorException {
        // Wait a millisecond (or two) until the current timestamp is not the same as the next.
        // Because the first byte is the last byte (reversed) of the current timestamp, the timestamps
        // have to differ to guarantee a different byte there.
        long now = System.currentTimeMillis();
        while (previousInvocation == now) {
            sleepAMillisecond();
            now = System.currentTimeMillis();
        }
        previousInvocation = now;

        // The above trick fails in rare cases, so perform an additional check to guarantee the desired
        // result.
        byte[] id = generator.generate();
        if (previous != null) {
            while (previous[0] == id[0]) {
                sleepAMillisecond();
                id = generator.generate();
            }
        }

        previous = id;
        return id;
    }

    private void sleepAMillisecond() {
        try {
            TimeUnit.MILLISECONDS.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Deque<byte[]> batch(int size) throws GeneratorException {
        Deque<byte[]> deck = new ArrayDeque<>();
        for (int i = 0; i < size; i++) {
            deck.add(generate());
        }
        return deck;
    }
}

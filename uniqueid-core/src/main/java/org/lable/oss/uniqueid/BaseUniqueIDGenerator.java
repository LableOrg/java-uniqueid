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

import org.lable.oss.uniqueid.bytes.Blueprint;
import org.lable.oss.uniqueid.bytes.IDBuilder;
import org.lable.oss.uniqueid.bytes.Mode;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * Generate short, possibly unique IDs based on the current timestamp.
 * <p>
 * Whether the IDs are truly unique or not depends on the scope of its use. If the combination of generator-ID and
 * cluster-ID passed to this class is unique — i.e., there is only one ID-generator using that specific combination of
 * generator-ID and cluster-ID within the confines of your computing environment at the moment you generate an ID —
 * then the IDs returned are unique.
 */
public class BaseUniqueIDGenerator implements IDGenerator {
    protected final GeneratorIdentityHolder generatorIdentityHolder;
    private final Mode mode;

    long previousTimestamp = 0;
    int sequence = 0;

    /**
     * Create a new UniqueIDGenerator instance.
     *
     * @param generatorIdentityHolder Generator identity holder.
     * @param mode                    Generator mode.
     */
    public BaseUniqueIDGenerator(GeneratorIdentityHolder generatorIdentityHolder, Mode mode) {
        this.generatorIdentityHolder = generatorIdentityHolder;
        this.mode = mode == null ? Mode.defaultMode() : mode;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized byte[] generate() throws GeneratorException {

        long now = System.currentTimeMillis();
        if (now == previousTimestamp) {
            sequence++;
        } else {
            sequence = 0;
        }
        if (sequence > Blueprint.MAX_SEQUENCE_COUNTER) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                return generate();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        previousTimestamp = now;

        Blueprint blueprint = new Blueprint(
                now,
                sequence,
                generatorIdentityHolder.getGeneratorId(),
                generatorIdentityHolder.getClusterId(),
                mode
        );

        return IDBuilder.build(blueprint);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Deque<byte[]> batch(int size) throws GeneratorException {
        Deque<byte[]> stack = new ArrayDeque<>();
        for (int i = 0; i < size; i++) {
            stack.add(generate());
        }
        return stack;
    }

    @Override
    public void close() throws IOException {
        generatorIdentityHolder.close();
    }
}

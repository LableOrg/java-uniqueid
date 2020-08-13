/*
 * Copyright © 2014 Lable (info@lable.nl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lable.oss.uniqueid;

import java.io.Closeable;
import java.util.Deque;

/**
 * Generate short, unique IDs.
 */
public interface IDGenerator extends Closeable {
    /**
     * Generate a fresh ID.
     *
     * @return The generated ID.
     * @throws GeneratorException Thrown when an ID could not be generated. In practice, this exception is usually only
     *                            thrown by the more complex implementations of {@link IDGenerator}.
     */
    byte[] generate() throws GeneratorException;

    /**
     * Generate a batch of IDs. This is the preferred way of generating IDs when you expect to use more than a few IDs.
     *
     * @param size How many IDs to generate, implementing classes may decide to limit the maximum number of IDs
     *             generated at a time.
     * @return A stack of IDs, containing {@code size} or fewer IDs.
     * @throws GeneratorException Thrown when an ID could not be generated. In practice, this exception is usually only
     *                            thrown by the more complex implementations of {@link IDGenerator}.
     */
    Deque<byte[]> batch(int size) throws GeneratorException;
}

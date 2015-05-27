package org.lable.util.uniqueid;

import java.util.Deque;

/**
 * Generate short, unique IDs.
 */
public interface IDGenerator {
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

package org.lable.util.uniqueid;

import java.util.Deque;

/**
 * Generate short, possibly unique ID's.
 */
public interface IDGenerator {
    /**
     * Generate a fresh ID.
     *
     * @return The generated ID.
     * @throws GeneratorException Thrown when an ID could not be generated. In practice,
     *                            this exception is usually only thrown by the more complex implementations of
     *                            {@link org.lable.util.uniqueid.IDGenerator}.
     */
    public byte[] generate() throws GeneratorException;

    /**
     * Generate a batch of ID's. This is the preferred way of generating ID's when you expect to use more than a few
     * ID's.
     *
     * @param size How many ID's to generate, implementing classes may decide to limit the maximum number of ID's
     *             generated at a time.
     * @return A stack of ID's, containing {@code size} or fewer ID's.
     * @throws GeneratorException Thrown when an ID could not be generated. In practice,
     *                            this exception is usually only thrown by the more complex implementations of
     *                            {@link org.lable.util.uniqueid.IDGenerator}.
     */
    public Deque<byte[]> batch(int size) throws GeneratorException;
}

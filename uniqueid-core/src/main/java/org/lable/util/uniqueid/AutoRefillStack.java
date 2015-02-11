package org.lable.util.uniqueid;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

/**
 * A caching wrapper around an {@link org.lable.util.uniqueid.IDGenerator} instance.
 * <p/>
 * This class will cache a bunch of generated IDs and automatically refill the stack when it runs out. By letting
 * this class handle the caching, calling classes can simply call {@link #generate()} whenever a new ID is needed,
 * without having to worry about any performance hit you might see when calling
 * {@link IDGenerator#generate()} repeatedly from a time-consuming loop.
 */
public class AutoRefillStack implements IDGenerator {

    static final int DEFAULT_BATCH_SIZE = 500;

    final int batchSize;
    final IDGenerator generator;
    final Deque<byte[]> idStack = new ArrayDeque<byte[]>();

    /**
     * Create a new AutoRefillStack.
     * <p/>
     * It's internal cache will be set to 500 IDs.
     *
     * @param generator The IDGenerator to wrap.
     */
    public AutoRefillStack(IDGenerator generator) {
        this(generator, DEFAULT_BATCH_SIZE);
    }

    /**
     * Create a new AutoRefillStack, with a specific batch size.
     *
     * @param generator The IDGenerator to wrap.
     * @param batchSize The amount of IDs to cache.
     */
    public AutoRefillStack(IDGenerator generator, int batchSize) {
        this.batchSize = batchSize;
        this.generator = generator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized byte[] generate() throws GeneratorException {
        return popOne();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized Deque<byte[]> batch(int size) throws GeneratorException {
        if (size < 0) {
            size = 0;
        }
        Deque<byte[]> batch = new ArrayDeque<byte[]>(size);
        while (size > 0) {
            batch.add(popOne());
            size--;
        }
        return batch;
    }

    /**
     * Grab a single ID from the stack. If the stack is empty, load up a new batch from the wrapped generator.
     *
     * @return A single ID.
     * @throws GeneratorException
     */
    byte[] popOne() throws GeneratorException {
        try {
            return idStack.pop();
        } catch (NoSuchElementException e) {
            // Cached stack is empty, load up a fresh stack.
            idStack.addAll(generator.batch(batchSize));
            return popOne();
        }
    }
}

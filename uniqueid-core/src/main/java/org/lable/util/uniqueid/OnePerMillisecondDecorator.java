package org.lable.util.uniqueid;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * Decorator for an {@link IDGenerator} that sleeps at least a millisecond between each invocation to guarantee ID
 * spread.
 * </p>
 * <p>
 * This is not normally necessary or desired, but can be useful when you want to generate several IDs, but you don't
 * want subsequent IDs to start with the same byte.
 * </p>
 * <p>
 * This class is of course significantly slower than using an undecorated generator.
 * </p>
 */
public class OnePerMillisecondDecorator implements IDGenerator {
    final IDGenerator generator;
    long previousInvocation = 0;

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
        long now = System.currentTimeMillis();
        while (previousInvocation == now) {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            now = System.currentTimeMillis();
        }
        previousInvocation = now;
        return generator.generate();
    }

    @Override
    public Deque<byte[]> batch(int size) throws GeneratorException {
        Deque<byte[]> deck = new ArrayDeque<byte[]>();
        for (int i = 0; i < size; i++) {
            deck.add(generate());
        }
        return deck;
    }
}

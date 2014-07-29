package org.lable.util.uniqueid;

import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class AutoRefillStackTest {
    Random random = new Random();

    @Test
    public void refillTest() throws GeneratorException {
        IDGenerator generator = mock(IDGenerator.class);
        Deque<byte[]> deck1 = new ArrayDeque<byte[]>(10);
        Deque<byte[]> deck2 = new ArrayDeque<byte[]>(10);
        for (int i = 0; i < 10; i++) {
            deck1.add(Long.toHexString(random.nextLong()).getBytes());
            deck2.add(Long.toHexString(random.nextLong()).getBytes());
        }
        when(generator.batch(10)).thenReturn(deck1).thenReturn(deck2);

        AutoRefillStack stack = new AutoRefillStack(generator, 10);

        // Grab 9 ID's.
        Deque<byte[]> deck = stack.batch(9);
        assertThat(deck.size(), is(9));

        byte[] id = stack.generate();
        assertThat(id, is(not(nullValue())));

        // This should cause the wrapped IDGenerator's #batch() to be called a second time.
        id = stack.generate();
        assertThat(id, is(not(nullValue())));

        verify(generator, times(2)).batch(10);
        verifyNoMoreInteractions(generator);
    }
}
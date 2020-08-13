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

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;


public class AutoRefillStackTest {
    Random random = new Random();

    @Test
    public void refillTest() throws GeneratorException {
        IDGenerator generator = mock(IDGenerator.class);
        Deque<byte[]> deck1 = new ArrayDeque<>(10);
        Deque<byte[]> deck2 = new ArrayDeque<>(10);
        for (int i = 0; i < 10; i++) {
            deck1.add(Long.toHexString(random.nextLong()).getBytes());
            deck2.add(Long.toHexString(random.nextLong()).getBytes());
        }
        when(generator.batch(10)).thenReturn(deck1).thenReturn(deck2);

        IDGenerator stack = AutoRefillStack.decorate(generator, 10);

        // Grab 9 IDs.
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

    @Test
    public void defaultConstructorTest() throws GeneratorException {
        IDGenerator generator = mock(IDGenerator.class);
        Deque<byte[]> dummyDeck = new ArrayDeque<byte[]>(AutoRefillStack.DEFAULT_BATCH_SIZE);
        for (int i = 0; i < AutoRefillStack.DEFAULT_BATCH_SIZE; i++) {
            dummyDeck.add(Long.toHexString(random.nextLong()).getBytes());
        }
        when(generator.batch(AutoRefillStack.DEFAULT_BATCH_SIZE)).thenReturn(dummyDeck);

        IDGenerator stack = AutoRefillStack.decorate(generator);

        // Call batch with a value that will cause it to return an empty list.
        // The wrapped generator should not be called.
        Deque<byte[]> ids = stack.batch(-1);
        assertThat(ids.size(), is(0));
        verify(generator, never()).batch(anyInt());

        // Trigger the wrapper to load up a fresh batch of IDs.
        stack.generate();
        assertThat(((AutoRefillStack) stack).idStack.size(), is(AutoRefillStack.DEFAULT_BATCH_SIZE - 1));
        verify(generator).batch(AutoRefillStack.DEFAULT_BATCH_SIZE);
    }
}
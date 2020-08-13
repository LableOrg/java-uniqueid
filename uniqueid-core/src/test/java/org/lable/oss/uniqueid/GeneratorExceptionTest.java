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

import java.io.IOException;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class GeneratorExceptionTest {
    @Test
    public void constructionTest() {
        assertThrows(GeneratorException.class, () -> {
            throw new GeneratorException();
        });
    }

    @Test
    public void constructionWithMessageTest() {
        assertThrows(
                "Hello!",
                GeneratorException.class,
                () -> {
                    throw new GeneratorException("Hello!");
                }
        );

    }

    @Test
    public void constructionWithMessageAndThrowableTest() {
        GeneratorException e = assertThrows(
                "Hello!",
                GeneratorException.class,
                () -> {
                    throw new GeneratorException("Hello!", new IOException("XXX"));
                }
        );
        assertThat(e.getCause(), is(instanceOf(IOException.class)));
    }

    @Test
    public void constructionWithThrowableTest() throws GeneratorException {
        GeneratorException e = assertThrows(
                GeneratorException.class,
                () -> {
                    throw new GeneratorException(new IOException("XXX"));
                }
        );
        assertThat(e.getCause(), is(instanceOf(IOException.class)));
    }
}
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


import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.isA;

public class GeneratorExceptionTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void constructionTest() throws GeneratorException {
        thrown.expect(GeneratorException.class);
        throw new GeneratorException();
    }

    @Test
    public void constructionWithMessageTest() throws GeneratorException {
        thrown.expect(GeneratorException.class);
        thrown.expectMessage("Hello!");
        throw new GeneratorException("Hello!");
    }

    @Test
    public void constructionWithMessageAndThrowableTest() throws GeneratorException {
        thrown.expect(GeneratorException.class);
        thrown.expectMessage("Hello!");
        thrown.expectCause(isA(IOException.class));
        throw new GeneratorException("Hello!", new IOException("XXX"));
    }

    @Test
    public void constructionWithThrowableTest() throws GeneratorException {
        thrown.expect(GeneratorException.class);
        thrown.expectCause(isA(IOException.class));
        throw new GeneratorException(new IOException("XXX"));
    }
}
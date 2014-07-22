package org.lable.util.uniqueid;


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
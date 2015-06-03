package org.lable.oss.uniqueid;

/**
 * General exception throwable by the public API of this project.
 */
public class GeneratorException extends Exception {
    public GeneratorException(String message) {
        super(message);
    }

    public GeneratorException(String message, Throwable cause) {
        super(message, cause);
    }

    public GeneratorException() {
        super();
    }

    public GeneratorException(Throwable cause) {
        super(cause);
    }
}

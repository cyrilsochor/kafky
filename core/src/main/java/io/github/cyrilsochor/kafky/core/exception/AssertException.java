package io.github.cyrilsochor.kafky.core.exception;

public class AssertException extends RuntimeException {

    public AssertException() {
        super();
    }

    public AssertException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public AssertException(String message, Throwable cause) {
        super(message, cause);
    }

    public AssertException(String message) {
        super(message);
    }

    public AssertException(Throwable cause) {
        super(cause);
    }

}

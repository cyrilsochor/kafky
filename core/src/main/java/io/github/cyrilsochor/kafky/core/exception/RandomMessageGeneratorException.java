package io.github.cyrilsochor.kafky.core.exception;

public class RandomMessageGeneratorException extends RuntimeException {

    public RandomMessageGeneratorException() {
        super();
    }

    public RandomMessageGeneratorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public RandomMessageGeneratorException(String message, Throwable cause) {
        super(message, cause);
    }

    public RandomMessageGeneratorException(String message) {
        super(message);
    }

    public RandomMessageGeneratorException(Throwable cause) {
        super(cause);
    }

}

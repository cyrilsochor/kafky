package io.github.cyrilsochor.kafky.core.exception;

public class InvalidSchemaException extends RuntimeException {

    public InvalidSchemaException(final String schemaJson, final Throwable cause) {
        super("Invalid json schema: " + schemaJson, cause);
    }

}

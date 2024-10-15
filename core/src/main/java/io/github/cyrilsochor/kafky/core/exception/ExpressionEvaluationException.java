package io.github.cyrilsochor.kafky.core.exception;

import static java.lang.String.format;

import com.ezylang.evalex.Expression;

public class ExpressionEvaluationException extends RuntimeException {

    private final Object target;
    private final Expression expression;

    public ExpressionEvaluationException(
            final Object target,
            final Expression expression,
            final Throwable cause) {
        super(format("Error evaluate target '%s' expression '%s'",
                target,
                expression.getExpressionString()),
                cause);
        this.target = target;
        this.expression = expression;
    }

    public Object getTarget() {
        return target;
    }

    public Expression getExpression() {
        return expression;
    }

}

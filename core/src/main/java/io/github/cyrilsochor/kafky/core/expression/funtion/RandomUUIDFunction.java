package io.github.cyrilsochor.kafky.core.expression.funtion;

import com.ezylang.evalex.Expression;
import com.ezylang.evalex.data.EvaluationValue;
import com.ezylang.evalex.functions.AbstractFunction;
import com.ezylang.evalex.parser.Token;

import java.util.UUID;

public class RandomUUIDFunction extends AbstractFunction {

    @Override
    public EvaluationValue evaluate(
            Expression expression, Token functionToken, EvaluationValue... parameterValues) {
        return expression.convertValue(UUID.randomUUID().toString());
    }

}

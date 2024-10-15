package io.github.cyrilsochor.kafky.core.expression.funtion;

import com.ezylang.evalex.Expression;
import com.ezylang.evalex.data.EvaluationValue;
import com.ezylang.evalex.functions.AbstractFunction;
import com.ezylang.evalex.functions.FunctionParameter;
import com.ezylang.evalex.parser.Token;

import java.util.Random;

@FunctionParameter(name = "origin")
@FunctionParameter(name = "bound")
public class RandomLongFunction extends AbstractFunction {

    protected final Random random = new Random();

    @Override
    public EvaluationValue evaluate(
            Expression expression, Token functionToken, EvaluationValue... parameterValues) {
        final long origin = parameterValues[0].getNumberValue().longValue();
        final long bound = parameterValues[1].getNumberValue().longValue();
        return expression.convertValue(random.nextLong(origin, bound));
    }

}

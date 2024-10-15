package io.github.cyrilsochor.kafky.core.expression.funtion;

import com.ezylang.evalex.Expression;
import com.ezylang.evalex.data.EvaluationValue;
import com.ezylang.evalex.functions.AbstractFunction;
import com.ezylang.evalex.functions.FunctionParameter;
import com.ezylang.evalex.parser.Token;

import java.util.List;

@FunctionParameter(name = "array")
public class SizeFunction extends AbstractFunction {

    @Override
    public EvaluationValue evaluate(
            Expression expression, Token functionToken, EvaluationValue... parameterValues) {
        final List<EvaluationValue> array = parameterValues[0].getArrayValue();
        return expression.convertValue(array.size());
    }

}

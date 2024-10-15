package io.github.cyrilsochor.kafky.core.expression.funtion;

import brave.ScopedSpan;
import brave.Tracing;
import brave.propagation.B3Propagation;
import brave.propagation.Propagation;
import brave.sampler.SamplerFunction;
import com.ezylang.evalex.Expression;
import com.ezylang.evalex.data.EvaluationValue;
import com.ezylang.evalex.functions.AbstractFunction;
import com.ezylang.evalex.functions.FunctionParameter;
import com.ezylang.evalex.parser.Token;

@FunctionParameter(name = "sampled")
public class NewB3Function extends AbstractFunction {

    protected static final String SPAN_NAME = "kafky";

    protected final Tracing tracing = Tracing.newBuilder().build();
    protected final Propagation<String> propagation = B3Propagation.newFactoryBuilder().build().get();

    @Override
    public EvaluationValue evaluate(
            Expression expression, Token functionToken, EvaluationValue... parameterValues) {
        final Boolean sampled = parameterValues[0].getBooleanValue();
        return expression.convertValue(newB3(sampled));
    }

    protected String newB3(final Boolean sampled) {
        final SamplerFunction<String> samplerFunction = x -> sampled;
        final ScopedSpan span = tracing.tracer().startScopedSpan(SPAN_NAME, samplerFunction, "");
        try {
            return brave.propagation.B3SingleFormat.writeB3SingleFormat(tracing.currentTraceContext().get());
        } finally {
            span.finish();
        }
    }

}

package io.github.cyrilsochor.kafky.core.global;

import com.ezylang.evalex.config.ExpressionConfiguration;
import com.ezylang.evalex.functions.FunctionIfc;
import io.github.cyrilsochor.kafky.api.component.Component;
import io.github.cyrilsochor.kafky.core.config.KafkyExpressionEngineConfig;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;

import java.lang.reflect.InvocationTargetException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ExpressionEngine implements Component {

    @SuppressWarnings("unchecked")
    public static ExpressionEngine of(final Map<Object, Object> cfg) throws InstantiationException, IllegalAccessException, IllegalArgumentException,
            InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
        final List<Entry<String, FunctionIfc>> additionalFunctions = getCustomFunctions(
                PropertiesUtils.getMap(cfg, KafkyExpressionEngineConfig.CUSTOM_FUNCTIONS));
        final ExpressionConfiguration expressionConfiguration = ExpressionConfiguration.builder()
                .dateTimeFormatters(List.of(DateTimeFormatter.ISO_INSTANT))
                .build()
                .withAdditionalFunctions(additionalFunctions.toArray(s -> new Map.Entry[s]));
        return new ExpressionEngine(expressionConfiguration);
    }

    protected static ArrayList<Entry<String, FunctionIfc>> getCustomFunctions(Map<Object, Object> cfgFunctions)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
            SecurityException, ClassNotFoundException {
        final ArrayList<Entry<String, FunctionIfc>> functions = new ArrayList<>();
        if (cfgFunctions != null) {
            for (Entry<Object, Object> f : cfgFunctions.entrySet()) {
                final FunctionIfc function = Class.forName(f.getValue().toString()).asSubclass(FunctionIfc.class).getConstructor().newInstance();
                functions.add(Map.entry(f.getKey().toString(), function));
            }
        }
        return functions;
    }

    protected final ExpressionConfiguration expressionConfiguration;

    protected ExpressionEngine(ExpressionConfiguration expressionConfiguration) {
        this.expressionConfiguration = expressionConfiguration;
    }

    public ExpressionConfiguration getExpressionConfiguration() {
        return expressionConfiguration;
    }

}

package io.github.cyrilsochor.kafky.core.config;

import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.addProperties;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.cyrilsochor.kafky.core.serde.Serdes;
import io.github.cyrilsochor.kafky.core.util.PropertiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("unchecked")
public class ConfigurationManager {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationManager.class);

    public static KafkyConfiguration readFile(final String cfgFile) throws IOException {
        final Path cfgPath = Path.of(cfgFile);
        if (!Files.isReadable(cfgPath)) {
            throw new IllegalArgumentException(String.format("Configuration file '%s' not found", cfgPath.toAbsolutePath().toString()));
        }

        LOG.info("Reading configuration file {}", cfgPath.toAbsolutePath());

        final ObjectMapper mapper = Serdes.getDefaultObjectMapper();
        final KafkyConfiguration cfg = mapper.readValue(Files.newBufferedReader(cfgPath), KafkyConfiguration.class);

        return cfg;
    }

    public static void merge(final KafkyConfiguration target, final KafkyConfiguration sourceLowerPriority) {
        mergeByKey(target.consumers(), sourceLowerPriority.consumers());
        mergeByKey(target.producers(), sourceLowerPriority.producers());
        addProperties(target.global(), sourceLowerPriority.global());
        addProperties(target.globalConsumers(), sourceLowerPriority.globalConsumers());
        addProperties(target.globalProducers(), sourceLowerPriority.globalProducers());
        addProperties(target.report(), sourceLowerPriority.report());
    }

    protected static void mergeByKey(final Map<Object, Object> target, final Map<Object, Object> source) {
        for (Entry<Object, Object> sourceConsumer : source.entrySet()) {
            final String key = (String) sourceConsumer.getKey();
            final Map<Object, Object> sourceValues = (Map<Object, Object>) sourceConsumer.getValue();
            final Map<Object, Object> targetValues = (Map<Object, Object>) target.get(key);
            if (targetValues == null) {
                target.put(key, sourceValues);
            } else {
                PropertiesUtils.addProperties(targetValues, sourceValues);
            }
        }
    }

}

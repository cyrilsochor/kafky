package io.github.cyrilsochor.kafky.core.runtime.job.producer;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.cyrilsochor.kafky.api.job.producer.RecordProducer;
import io.github.cyrilsochor.kafky.core.config.KafkyExpressionEngineConfig;
import io.github.cyrilsochor.kafky.core.expression.funtion.NewB3Function;
import io.github.cyrilsochor.kafky.core.expression.funtion.RandomLongFunction;
import io.github.cyrilsochor.kafky.core.expression.funtion.RandomUUIDFunction;
import io.github.cyrilsochor.kafky.core.expression.funtion.SizeFunction;
import io.github.cyrilsochor.kafky.core.global.ExpressionEngine;
import io.github.cyrilsochor.kafky.core.runtime.KafkyRuntime;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class HeaderDecoratorTest {

    private static final String SOURCE_TOPIC = "Ttopic";
    private static final Integer SOURCE_PARTITION = 19;
    private static final Long SOURCE_TIMESTAMP = 163432123l;
    private static final String SOURCE_KEY = "Tkey";
    private static final String SOURCE_VALUE = "Tvalue";
    private static final Headers SOURCE_HEADERS;
    static {
        SOURCE_HEADERS = new RecordHeaders();
        SOURCE_HEADERS.add("age", "36".getBytes());
        SOURCE_HEADERS.add("children", "Anna".getBytes());
        SOURCE_HEADERS.add("children", "Bery".getBytes());
    }

    @Test
    void emptyCfg() throws Exception {
        final ExpressionEngine expressionEngine = ExpressionEngine.of(emptyMap());
        assertNull(ExpressionDecorator.of(emptyMap(), new KafkyRuntime() {
            @Override
            public <I> I getGlobalComponentByType(Class<? extends I> componentInterface) {
                return (I) expressionEngine;
            }
        }));
    }

    @Test
    void constant() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("foo", "\"bar\"")), createSourceDefault());
        assertNotNull(rec);
        assertEquals("bar", new String(rec.headers().lastHeader("foo").value()));
        assertEquals("36", new String(rec.headers().lastHeader("age").value()));
    }

    @Test
    void constantJson() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("foo", """
                        "{\\"app\\":\\"banking\\"}" """)), createSourceDefault());
        assertNotNull(rec);
        assertEquals("""
                {\"app\":\"banking\"}""", new String(rec.headers().lastHeader("foo").value()));
    }

    @Test
    void nullAge() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("age", "NULL")), createSourceDefault());
        assertNotNull(rec);
        assertNull(rec.headers().lastHeader("age"));
    }

    @Test
    void unknownFunction() throws Exception {
        decorateException(Map.of("headers",
                Map.of("foo", "bar()")), createSourceDefault(), "Undefined function 'bar'");
    }

    @Test
    void oldValueOne() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("children", "oldValues[1]")), createSourceDefault());
        assertNotNull(rec);
        assertEquals("Bery", new String(rec.headers().lastHeader("children").value()));
    }

    @Test
    void oldValueList() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("age", "oldValues")), createSourceDefault());
        assertNotNull(rec);
        assertEquals("36", new String(rec.headers().lastHeader("age").value()));
    }

    @Test
    void math() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("age", "3*10+9")), createSourceDefault());
        assertNotNull(rec);
        assertEquals("39", new String(rec.headers().lastHeader("age").value()));
    }

    @Test
    void uuid() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("id", "RANDOM_UUID()")), createSourceDefault());
        final String actual = new String(rec.headers().lastHeader("id").value());
        assertEquals(36, actual.length());
        assertEquals('-', actual.charAt(8));
    }

    @Test
    void size() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("children", "SIZE(oldValues)")), createSourceDefault());
        assertEquals("2", new String(rec.headers().lastHeader("children").value()));
    }

    @Test
    void now() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("timestamp", "DT_DATE_FORMAT(DT_NOW())")), createSourceDefault());
        final String actual = new String(rec.headers().lastHeader("timestamp").value());
        assertEquals(30, actual.length());
        assertEquals('-', actual.charAt(4));
        assertEquals('-', actual.charAt(7));
        assertEquals('T', actual.charAt(10));
        assertEquals('Z', actual.charAt(29));
    }

    @Test
    void propertiesValue() throws Exception {
        final String expression = "configuration.properties.\"bootstrap.servers\"";
        final ProducerRecord<Object, Object> rec = decorate(new HashMap<>(Map.of(
                "properties",
                new HashMap<>(Map.of("bootstrap.servers", "PLAINTEXT://localhost:9093")),
                "headers", new HashMap<>(Map.of(
                        "broker", expression,
                        "age", expression)))),
                createSourceDefault());
        assertNotNull(rec);
        assertEquals("PLAINTEXT://localhost:9093", new String(rec.headers().lastHeader("age").value()));
    }

    @Test
    void propertiesValueIfOld() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(new HashMap<>(Map.of(
                "properties",
                new HashMap<>(Map.of("bootstrap.servers", "PLAINTEXT://localhost:9093")),
                "headers", new HashMap<>(Map.of("children", "IF(SIZE(oldValues)>0,configuration.properties.\"bootstrap.servers\",NULL)")))),
                createSourceDefault());
        assertNotNull(rec);
        assertEquals("PLAINTEXT://localhost:9093", new String(rec.headers().lastHeader("children").value()));
        assertNull(rec.headers().lastHeader("broker"));
    }

    @Test
    void newB3SampledTrue() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("b3", "NEW_B3(TRUE)")), createSourceDefault());
        final String actual = new String(rec.headers().lastHeader("b3").value());
        assertEquals(35, actual.length());
        assertEquals("-1", actual.substring(33));
    }

    @Test
    void newB3SampledFalse() throws Exception {
        final ProducerRecord<Object, Object> rec = decorate(Map.of("headers",
                Map.of("b3", "NEW_B3(FALSE)")), createSourceDefault());
        final String actual = new String(rec.headers().lastHeader("b3").value());
        assertEquals(35, actual.length());
        assertEquals("-0", actual.substring(33));
    }

    protected ProducerRecord<Object, Object> decorate(
            final Map<Object, Object> cfg,
            final ProducerRecord<Object, Object> sourceRecord) throws Exception {
        final RecordProducer decorator = createHeaderDecorator(cfg, sourceRecord);
        return decorator.produce();
    }

    protected void decorateException(
            final Map<Object, Object> cfg,
            final ProducerRecord<Object, Object> sourceRecord,
            final String expectedExceptionMessage) throws Exception {
        final RecordProducer decorator = createHeaderDecorator(cfg, sourceRecord);
        final Exception exception = assertThrows(Exception.class, () -> decorator.produce());
        assertEquals(expectedExceptionMessage, exception.getMessage());
    }

    protected RecordProducer createHeaderDecorator(final Map<Object, Object> cfg, final ProducerRecord<Object, Object> sourceRecord)
            throws Exception {
        final Map<Object, Object> fullCfg = new HashMap<>();
        fullCfg.put(KafkyExpressionEngineConfig.CUSTOM_FUNCTIONS, Map.of(
                "RANDOM_UUID", RandomUUIDFunction.class.getName(),
                "RANDOM_LONG", RandomLongFunction.class.getName(),
                "SIZE", SizeFunction.class.getName(),
                "NEW_B3", NewB3Function.class.getName()
        ));
        final ExpressionEngine expressionEngine = ExpressionEngine.of(fullCfg);
        final RecordProducer decorator = ExpressionDecorator.of(cfg, new KafkyRuntime() {
            @Override
            public <I> I getGlobalComponentByType(Class<? extends I> componentInterface) {
                return (I) expressionEngine;
            }
        });
        assertNotNull(decorator);
        decorator.setChainNext(new ConstantRecordProducer(sourceRecord));
        decorator.init();
        return decorator;
    }

    protected ProducerRecord<Object, Object> createSourceDefault() {
        final ProducerRecord<Object, Object> sourceRecord = new ProducerRecord<>(
                SOURCE_TOPIC,
                SOURCE_PARTITION,
                SOURCE_TIMESTAMP,
                SOURCE_KEY,
                SOURCE_VALUE,
                SOURCE_HEADERS);
        return sourceRecord;
    }

}

package io.github.cyrilsochor.kafky.core.util;

import static io.github.cyrilsochor.kafky.core.util.PropertiesUtils.appendPathSuffix;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PropertiesUtilsTest {

    @Test
    void withSuffix() {
        assertEquals("xxx-localhost.pdf", appendPathSuffix("xxx.pdf", "-localhost"));
    }

    @Test
    void withMultipleDots() {
        assertEquals("a.b/xxx-localhost.pdf", appendPathSuffix("a.b/xxx.pdf", "-localhost"));
    }

    @Test
    void withoutExtension() {
        assertEquals("xxx", appendPathSuffix("xxx", "-localhost"));
    }

}

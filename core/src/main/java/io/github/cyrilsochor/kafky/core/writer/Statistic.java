package io.github.cyrilsochor.kafky.core.writer;

import io.github.cyrilsochor.kafky.core.writer.StatisticsWriter.Flag;

import java.util.Set;

public record Statistic(
        String name,
        String valueText,
        Set<Flag> flags) {

}

package io.github.cyrilsochor.kafky.core.global;

import static io.github.cyrilsochor.kafky.api.job.JobState.MEASURING_RESPONSE_TIME;
import static java.time.temporal.ChronoUnit.MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.cyrilsochor.kafky.core.global.InMemoryPairMatcher.PhaseStatistic;
import io.github.cyrilsochor.kafky.core.stats.Statistics;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;

class InMemoryPairMatcherTest {

    @Test
    void testCookResponseTimeStatisticsEven() throws IOException {
        final InMemoryPairMatcher matcher = new InMemoryPairMatcher(null, null, null, null, null, null, null, null, null);
        final Statistics stats = new Statistics();
        final PhaseStatistic phaseStats = new PhaseStatistic(MEASURING_RESPONSE_TIME);
        phaseStats.durations = new ArrayList<Long>(Arrays.asList(271l, 242l, 275l, 374l, 322l, 328l, 281l, 284l, 295l, 244l, 289l, 210l, 245l, 245l,
                231l, 282l, 344l, 217l, 283l, 366l, 359l, 305l, 240l, 208l, 232l, 266l, 241l, 191l, 251l, 255l, 252l, 214l, 253l, 269l, 227l, 230l,
                206l, 308l, 351l, 259l, 246l, 335l, 225l, 224l, 353l, 218l, 218l, 250l, 254l, 291l));
        matcher.cookResponseTimeStatistics(stats, phaseStats);
        assertEquals(Duration.of(191, MILLIS), stats.getResponseMinDuration());
        assertEquals(Duration.of(267, MILLIS), stats.getResponseAvgDuration());
        assertEquals(Duration.of(253, MILLIS), stats.getResponseMedDuration());
        assertEquals(Duration.of(374, MILLIS), stats.getResponseMaxDuration());
    }

    @Test
    void testCookResponseTimeStatisticsOdd() throws IOException {
        final InMemoryPairMatcher matcher = new InMemoryPairMatcher(null, null, null, null, null, null, null, null, null);
        final Statistics stats = new Statistics();
        final PhaseStatistic phaseStats = new PhaseStatistic(MEASURING_RESPONSE_TIME);
        phaseStats.durations = new ArrayList<Long>(Arrays.asList(1l, 16l, 158l, 365l, 84l));
        matcher.cookResponseTimeStatistics(stats, phaseStats);
        assertEquals(Duration.of(1, MILLIS), stats.getResponseMinDuration());
        assertEquals(Duration.of(124, MILLIS), stats.getResponseAvgDuration());
        assertEquals(Duration.of(84, MILLIS), stats.getResponseMedDuration());
        assertEquals(Duration.of(365, MILLIS), stats.getResponseMaxDuration());
    }

}

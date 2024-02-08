package timely.store.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.tserver.compaction.CompactionPlan;
import org.apache.accumulo.tserver.compaction.CompactionStrategy;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.junit.Test;

import timely.test.CompactionRequestBuilder;

public class MetricCompactionStrategyTest {

    private static final long TIME_MILLIS = new GregorianCalendar(2019, Calendar.JANUARY, 1).getTimeInMillis();

    @Test(expected = IllegalArgumentException.class)
    public void initThrowsWhenNoDefaultAgeOff() {
        Map<String,String> config = new HashMap<>();
        MetricCompactionStrategy strategy = new MetricCompactionStrategy();
        strategy.init(config);
        CompactionRequestBuilder builder = new CompactionRequestBuilder().endKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(11))
                        .prevEndKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(12)).file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                        .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        MajorCompactionRequest request = builder.build();
        strategy.shouldCompact(request);
    }

    @Test
    public void minAgeOffOverridesMajcDefaultWillUseMaximum() throws IOException {
        Map<String,String> config = new HashMap<>();
        config.put(MetricCompactionStrategy.MIN_AGEOFF_KEY, Long.toString(TimeUnit.DAYS.toMillis(10)));
        CompactionStrategy strategy = newStrategy(config);
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(13)
                .endKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(11))
                .prevEndKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(12))
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    @Test
    public void minAgeOffOverridesMajcDefaultNotCompact() throws IOException {
        Map<String,String> config = new HashMap<>();
        config.put(MetricCompactionStrategy.MIN_AGEOFF_KEY, Long.toString(TimeUnit.DAYS.toMillis(13)));
        CompactionStrategy strategy = newStrategy(config);
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(11))
                .prevEndKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(12))
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    @Test
    public void compactFilesIfKeyIsPastAgeOff() throws IOException {
        String f1 = "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf";
        String f2 = "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf";
        String f3 = "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao6.rf";
        CompactionStrategy strategy = newStrategy();

        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetric("sys", TimeUnit.DAYS.toMillis(11))
                .prevEndKeyMetric("sys", TimeUnit.DAYS.toMillis(12))
                .file(f1, 12, 1)
                .file(f2, 11, 1)
                .file(f3, 16, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertTrue(shouldCompact);
        assertNotNull(plan);

        assertEquals(3, plan.inputFiles.size());
        assertEquals(f1, plan.inputFiles.get(0).getPathStr());
        assertEquals(f2, plan.inputFiles.get(1).getPathStr());
        assertEquals(f3, plan.inputFiles.get(2).getPathStr());
    }

    @Test
    public void noCompactionIfPrevKeyIsDifferent() throws IOException {
        CompactionStrategy strategy = newStrategy();
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetric("sys.mem", TimeUnit.DAYS.toMillis(11))
                .prevEndKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(12))
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    @Test
    public void noCompactionIfPrevKeyIsNull() throws IOException {
        CompactionStrategy strategy = newStrategy();
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetric("sys", TimeUnit.DAYS.toMillis(11))
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    @Test
    public void noCompactionWhenExceedsButDoesNotHaveFiles() throws IOException {
        CompactionStrategy strategy = newStrategy();
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetric("sys", TimeUnit.DAYS.toMillis(11))
                .prevEndKeyMetric("sys", TimeUnit.DAYS.toMillis(12));

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    @Test
    public void noCompactionIfKeyIsNotOffset() throws IOException {
        CompactionStrategy strategy = newStrategy();
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetricNonOffset("sys.mem", null)
                .prevEndKeyMetricNonOffset("sys.cpu", null)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    @Test
    public void noCompactionIfKeyIsWrongOffset() throws IOException {
        CompactionStrategy strategy = newStrategy();
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetricNonOffset("sys.cpu", "test2")
                .prevEndKeyMetricNonOffset("sys.cpu", "test1")
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    @Test
    public void compactWithLogOnlyDoesNotCompact() throws IOException {
        Map<String,String> defaults = new HashMap<>();
        defaults.put(MetricCompactionStrategy.LOG_ONLY_KEY, "true");
        CompactionStrategy strategy = newStrategy(defaults);
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(11))
                .prevEndKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(12))
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    @Test
    public void compactWithFilterPrefixWillCompact() throws IOException {
        Map<String,String> defaults = new HashMap<>();
        defaults.put(MetricCompactionStrategy.FILTER_PREFIX_KEY, "sys.cp");
        CompactionStrategy strategy = newStrategy(defaults);
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(11))
                .prevEndKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(12))
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertTrue(shouldCompact);
        assertNotNull(plan);

        assertEquals(2, plan.inputFiles.size());
    }

    @Test
    public void compactWithoutFilterPrefixWillNotCompact() throws IOException {
        Map<String,String> defaults = new HashMap<>();
        defaults.put(MetricCompactionStrategy.FILTER_PREFIX_KEY, "sys.mem");
        CompactionStrategy strategy = newStrategy(defaults);
        // @formatter:off
        CompactionRequestBuilder builder = newRequestBuilder(10)
                .endKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(11))
                .prevEndKeyMetric("sys.cpu", TimeUnit.DAYS.toMillis(12))
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", 1, 1)
                .file("hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao7.rf", 1, 1);

        // @formatter:on
        MajorCompactionRequest request = builder.build();
        boolean shouldCompact = strategy.shouldCompact(request);
        CompactionPlan plan = strategy.getCompactionPlan(request);

        assertFalse(shouldCompact);
        assertNull(plan);
    }

    private static CompactionRequestBuilder newRequestBuilder(int defaultAgeOffDays) {
        return new CompactionRequestBuilder(TIME_MILLIS).tableProperties(Property.TABLE_ITERATOR_MAJC_PREFIX.getKey() + "ageoffmetrics.opt.ageoff.default",
                        Long.toString(TimeUnit.DAYS.toMillis(defaultAgeOffDays)));
    }

    private static MetricCompactionStrategy newStrategy() {
        return newStrategy(new TreeMap<>());
    }

    private static MetricCompactionStrategy newStrategy(Map<String,String> config) {
        if (!config.containsKey(MetricCompactionStrategy.LOG_ONLY_KEY)) {
            config.put(MetricCompactionStrategy.LOG_ONLY_KEY, "false");
        }
        MetricCompactionStrategy strategy = new MetricCompactionStrategy();
        strategy.setExplicitTime(TIME_MILLIS);
        strategy.init(config);
        return strategy;
    }
}

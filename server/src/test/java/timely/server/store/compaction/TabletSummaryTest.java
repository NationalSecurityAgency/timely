package timely.server.store.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.stat.descriptive.StatisticalSummary;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Lists;

import timely.accumulo.MetricAdapter;
import timely.server.store.compaction.util.MetadataAccumulator;
import timely.server.store.compaction.util.TabletStatistic;
import timely.server.store.compaction.util.TabletStatisticType;
import timely.server.store.compaction.util.TabletSummary;

public class TabletSummaryTest {

    private static final long DEFAULT_TIME_MILLIS = new GregorianCalendar(2019, Calendar.JANUARY, 1).getTimeInMillis();

    @Test
    public void totalsMatch() {
        Text r1 = new Text(MetricAdapter.encodeRowKey("sys.cpu", DEFAULT_TIME_MILLIS));
        Text r2 = new Text(MetricAdapter.encodeRowKey("sys.cpu", DEFAULT_TIME_MILLIS));
        Text r3 = new Text(MetricAdapter.encodeRowKey("sys.mem", DEFAULT_TIME_MILLIS));

        MetadataAccumulator.Entry e1 = new MetadataAccumulator.Entry(r1);
        MetadataAccumulator.Entry e2 = new MetadataAccumulator.Entry(r2);
        MetadataAccumulator.Entry e3 = new MetadataAccumulator.Entry(r3);

        e1.addFile(800);
        e1.addFile(100);
        e2.addFile(100);
        e2.addFile(200);
        e3.addFile(500);

        // @formatter:off
        TabletSummary summary = TabletSummary.newBuilder(Lists.newArrayList(e1, e2, e3))
                .disableTabletRowCheckFilter()
                .build();

        // @formatter:on
        assertEquals(3, summary.totalTablets());
        assertEquals(2, summary.totalTabletPrefixes());

        List<TabletStatistic> largest = Lists.newArrayList(summary.findTabletPrefixesByLargest(3));
        assertEquals(2, largest.size());
        assertEquals(1200, largest.get(0).totalSize());
        assertEquals(500, largest.get(1).totalSize());
    }

    @Test
    public void statsComputedCorrectly() {
        long a1 = TimeUnit.HOURS.toMillis(1);
        long a2 = TimeUnit.HOURS.toMillis(2);

        long ts1 = DEFAULT_TIME_MILLIS - a1;
        long ts2 = DEFAULT_TIME_MILLIS - a2;

        Text r1 = new Text(MetricAdapter.encodeRowKey("sys.cpu", ts1));
        Text r2 = new Text(MetricAdapter.encodeRowKey("sys.cpu", ts2));
        Text r3 = new Text(MetricAdapter.encodeRowKey("sys.mem", ts1));

        MetadataAccumulator.Entry e1 = new MetadataAccumulator.Entry(r1);
        MetadataAccumulator.Entry e2 = new MetadataAccumulator.Entry(r2);
        MetadataAccumulator.Entry e3 = new MetadataAccumulator.Entry(r3);

        e1.addFile(800);
        e1.addFile(100);
        e2.addFile(100);
        e2.addFile(200);
        e3.addFile(500);

        // @formatter:off
        TabletSummary summary = TabletSummary.newBuilder(Lists.newArrayList(e1, e2, e3))
                .currentTime(DEFAULT_TIME_MILLIS)
                .disableTabletRowCheckFilter()
                .build();

        // @formatter:on
        List<TabletStatistic> largest = Lists.newArrayList(summary.findTabletPrefixesByLargest(3));
        List<TabletStatistic> oldest = Lists.newArrayList(summary.findTabletPrefixesByOldest(3));

        assertEquals(2, largest.size());
        assertEquals(1200, largest.get(0).totalSize());
        assertEquals(500, largest.get(1).totalSize());

        assertEquals(2, oldest.size());
        assertEquals(a2, oldest.get(0).maxAge());
        assertEquals(a1, oldest.get(1).maxAge());
    }

    @Test
    public void aggregateComputes() {
        long a1 = TimeUnit.HOURS.toMillis(1);
        long a2 = TimeUnit.HOURS.toMillis(2);

        long ts1 = DEFAULT_TIME_MILLIS - a1;
        long ts2 = DEFAULT_TIME_MILLIS - a2;

        Text r1 = new Text(MetricAdapter.encodeRowKey("sys.cpu", ts1));
        Text r2 = new Text(MetricAdapter.encodeRowKey("sys.cpu", ts2));
        Text r3 = new Text(MetricAdapter.encodeRowKey("sys.mem", ts1));

        MetadataAccumulator.Entry e1 = new MetadataAccumulator.Entry(r1);
        MetadataAccumulator.Entry e2 = new MetadataAccumulator.Entry(r2);
        MetadataAccumulator.Entry e3 = new MetadataAccumulator.Entry(r3);

        e1.addFile(800);
        e1.addFile(100);
        e2.addFile(100);
        e2.addFile(200);
        e1.addFile(500);

        // @formatter:off
        TabletSummary summary = TabletSummary.newBuilder(Lists.newArrayList(e1, e2, e3))
                .currentTime(DEFAULT_TIME_MILLIS)
                .disableTabletRowCheckFilter()
                .build();

        // @formatter:on
        Map<TabletStatisticType,StatisticalSummary> map = summary.aggregateSummary();
        StatisticalSummary size = map.get(TabletStatisticType.SIZE);
        StatisticalSummary time = map.get(TabletStatisticType.TIME);

        assertEquals(1700, (long) size.getSum());
        assertEquals(a2, (long) time.getMax());
        assertEquals(a1, (long) time.getMin());
    }

    @Test
    public void initialTabletsArePrunedByDefault() {
        // test that the filter does not include
        // the differing tablet rows; the compaction strategy
        // should ignore

        long a1 = TimeUnit.DAYS.toMillis(365);
        long a2 = TimeUnit.HOURS.toMillis(2);

        long ts1 = DEFAULT_TIME_MILLIS - a1;
        long ts2 = DEFAULT_TIME_MILLIS - a2;

        Text r1 = new Text(MetricAdapter.encodeRowKey("sys.cpu", ts1));
        Text r2 = new Text(MetricAdapter.encodeRowKey("sys.cpu", ts2));

        MetadataAccumulator.Entry e1 = new MetadataAccumulator.Entry(r1);
        MetadataAccumulator.Entry e2 = new MetadataAccumulator.Entry(r2);

        e1.addFile(800);
        e2.addFile(100);
        e2.setTablePrev(r1);

        // @formatter:off
        TabletSummary summary = TabletSummary.newBuilder(Lists.newArrayList(e1, e2))
                .currentTime(DEFAULT_TIME_MILLIS)
                .build();

        // @formatter:on
        assertEquals(1, summary.totalTablets());
        assertEquals(1, summary.totalTabletPrefixes());

        Optional<TabletStatistic> stat = summary.getSummary().stream().findAny();

        // verify that initial stat was pruned from results
        // this is the default behavior
        assertTrue(stat.isPresent());
        assertEquals(a2, stat.get().maxAge());
    }
}

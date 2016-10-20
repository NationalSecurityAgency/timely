package timely.store.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Before;
import org.junit.Test;

import timely.adapter.accumulo.MetricAdapter;
import timely.model.Metric;
import timely.model.Tag;

public class TimeSeriesGroupingIteratorTest extends IteratorTestBase {

    private TreeMap<Key, Value> table = new TreeMap<Key, Value>();
    private static final List<Tag> tags = new ArrayList<>();
    static {
        tags.add(new Tag("rack", "r1"));
    }

    @Before
    public void setup() {
        table.clear();
        long ts = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            ts += 1000;
            Metric m = new Metric("sys.cpu.user", ts, i * 1.0D, tags);
            byte[] row = MetricAdapter.encodeRowKey(m);
            Key k = new Key(row, tags.get(0).join().getBytes(StandardCharsets.UTF_8), new byte[0], new byte[0], ts);
            Value v = new Value(MetricAdapter.encodeValue(m.getValue().getMeasure()));
            table.put(k, v);
        }
    }

    @Test
    public void testMovingAverage() throws Exception {
        SortedMapIterator source = new SortedMapIterator(table);
        TimeSeriesGroupingIterator iter = new TimeSeriesGroupingIterator();
        IteratorSetting settings = new IteratorSetting(100, TimeSeriesGroupingIterator.class);
        settings.addOption(TimeSeriesGroupingIterator.FILTER, "0.20,0.20,0.20,0.20,0.20");
        iter.init(source, settings.getOptions(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, true);

        for (int i = 4; i < 100; i++) {
            assertTrue(iter.hasTop());
            System.out.println(i);
            double expected = expectedMovingAverage(new double[] { i - 4, i - 3, i - 2, i - 1, i });
            assertEquals(expected, MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
            iter.next();
        }
        assertFalse(iter.hasTop());
    }

    public double expectedMovingAverage(double[] vals) {
        double result = 0.0D;
        for (int i = 0; i < vals.length; i++) {
            result += (0.20D * vals[i]);
        }
        return result;
    }

    @Test
    public void testMultipleTimeSeriesMovingAverage() throws Exception {
        table.clear();
        long ts = System.currentTimeMillis();
        List<Tag> tags1 = new ArrayList<>();
        tags1.add(new Tag("host", "r01n01"));
        List<Tag> tags2 = new ArrayList<>();
        tags2.add(new Tag("host", "r01n02"));
        for (int i = 0; i < 100; i++) {
            ts += 1000;
            Metric m = new Metric("sys.cpu.user", ts, i * 1.0D, tags1);
            byte[] row = MetricAdapter.encodeRowKey(m);
            Key k = new Key(row, tags1.get(0).join().getBytes(StandardCharsets.UTF_8), new byte[0], new byte[0], ts);
            Value v = new Value(MetricAdapter.encodeValue(m.getValue().getMeasure()));
            table.put(k, v);
            Metric m2 = new Metric("sys.cpu.user", ts, i * 2.0D, tags2);
            byte[] row2 = MetricAdapter.encodeRowKey(m2);
            Key k2 = new Key(row2, tags2.get(0).join().getBytes(StandardCharsets.UTF_8), new byte[0], new byte[0], ts);
            Value v2 = new Value(MetricAdapter.encodeValue(m2.getValue().getMeasure()));
            table.put(k2, v2);
        }
        SortedMapIterator source = new SortedMapIterator(table);
        TimeSeriesGroupingIterator iter = new TimeSeriesGroupingIterator();
        IteratorSetting settings = new IteratorSetting(100, TimeSeriesGroupingIterator.class);
        settings.addOption(TimeSeriesGroupingIterator.FILTER, "0.20,0.20,0.20,0.20,0.20");
        iter.init(source, settings.getOptions(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, true);

        for (int i = 4; i < 100; i++) {
            assertTrue(iter.hasTop());
            double expected = expectedMovingAverage(new double[] { i - 4, i - 3, i - 2, i - 1, i });
            assertEquals(expected, MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
            iter.next();
            assertTrue(iter.hasTop());
            double expected2 = expectedMovingAverage(new double[] { (i - 4) * 2, (i - 3) * 2, (i - 2) * 2, (i - 1) * 2,
                    i * 2 });
            assertEquals(expected2, MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
            iter.next();
        }
        assertFalse(iter.hasTop());

    }

}

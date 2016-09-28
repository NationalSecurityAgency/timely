package timely.store.iterators;

import static org.junit.Assert.*;

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
import timely.store.iterators.SlidingWindowIterator;

public class SlidingWindowTest extends IteratorTestBase {

    private TreeMap<Key, Value> table = new TreeMap<Key, Value>();
    private static final List<Tag> tags = new ArrayList<>();
    static {
        tags.add(new Tag("rack", "r1"));
    }

    @Before
    public void setup() {
        long ts = System.currentTimeMillis();
        for (int i = 1; i <= 100; i++) {
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
        SlidingWindowIterator iter = new SlidingWindowIterator();
        IteratorSetting settings = new IteratorSetting(100, SlidingWindowIterator.class);
        settings.addOption(SlidingWindowIterator.FILTER, "0.20,0.20,0.20,0.20,0.20");
        iter.init(source, settings.getOptions(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, true);
        assertTrue(iter.hasTop());
        assertEquals(expectedMovingAverage(new double[] { 1.0, 1.0, 1.0, 1.0, 1.0 }),
                MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
        iter.next();
        assertTrue(iter.hasTop());
        assertEquals(expectedMovingAverage(new double[] { 1.0, 1.0, 1.0, 1.0, 2.0 }),
                MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
        iter.next();
        assertTrue(iter.hasTop());
        assertEquals(expectedMovingAverage(new double[] { 1.0, 1.0, 1.0, 2.0, 3.0 }),
                MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
        iter.next();
        assertTrue(iter.hasTop());
        assertEquals(expectedMovingAverage(new double[] { 1.0, 1.0, 2.0, 3.0, 4.0 }),
                MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
        iter.next();
        for (int i = 5; i <= 100; i++) {
            double expected = expectedMovingAverage(new double[] { i - 4, i - 3, i - 2, i - 1, i });
            assertTrue(iter.hasTop());
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
    public void testRate() throws Exception {
        SortedMapIterator source = new SortedMapIterator(table);
        SlidingWindowIterator iter = new SlidingWindowIterator();
        IteratorSetting settings = new IteratorSetting(100, SlidingWindowIterator.class);
        settings.addOption(SlidingWindowIterator.FILTER, "-1.0,1.0");
        iter.init(source, settings.getOptions(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, true);
        assertTrue(iter.hasTop());
        assertEquals(0.0D, MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
        iter.next();
        for (int i = 2; i <= 100; i++) {
            assertTrue(iter.hasTop());
            assertEquals(1.0D, MetricAdapter.decodeValue(iter.getTopValue().get()), 0.0D);
            iter.next();
        }
        assertFalse(iter.hasTop());

    }

}

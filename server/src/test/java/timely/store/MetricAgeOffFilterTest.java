package timely.store;

import java.util.HashMap;

import static org.junit.Assert.*;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import timely.adapter.accumulo.MetricAdapter;
import timely.model.Metric;

public class MetricAgeOffFilterTest {

    private static final Long TEST_TIME = System.currentTimeMillis();
    private static final Integer ONE_DAY = 86400000;

    @Test(expected = IllegalArgumentException.class)
    public void testDefaultMissing() throws Exception {
        MetricAgeOffFilter filter = new MetricAgeOffFilter();
        HashMap<String, String> options = new HashMap<>();
        filter.init(null, options, null);
    }

    @Test
    public void testDefault() throws Exception {
        MetricAgeOffFilter filter = new MetricAgeOffFilter();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetricAgeOffFilter.AGE_OFF_PREFIX + "default", Integer.toString(1 * ONE_DAY));
        filter.init(null, options, null);
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME), new byte[0],
                new byte[0], new byte[0], TEST_TIME), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 1), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 1), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 2), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 2), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 3), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 3), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 4), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 4), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 5), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 5), null));
    }

    @Test
    public void testMixed() throws Exception {
        MetricAgeOffFilter filter = new MetricAgeOffFilter();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetricAgeOffFilter.AGE_OFF_PREFIX + "default", Integer.toString(1 * ONE_DAY));
        filter.init(null, options, null);
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME), new byte[0],
                new byte[0], new byte[0], TEST_TIME), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME + 1), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 1), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME + 2), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 2), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME + 3), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 3), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME + 4), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 4), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME + 5), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 5), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME), new byte[0],
                new byte[0], new byte[0], TEST_TIME), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 1), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 1), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 2), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 2), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 3), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 3), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 4), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 4), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 5), new byte[0],
                new byte[0], new byte[0], TEST_TIME + 5), null));
    }

    @Test
    public void testAgeoffMixed() throws Exception {
        MetricAgeOffFilter filter = new MetricAgeOffFilter();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetricAgeOffFilter.AGE_OFF_PREFIX + "default", Integer.toString(1 * ONE_DAY));
        options.put(MetricAgeOffFilter.AGE_OFF_PREFIX + "sys.cpu.user", Integer.toString(2 * ONE_DAY));
        filter.init(null, options, null);
        assertFalse(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME - (3 * ONE_DAY)),
                new byte[0], new byte[0], new byte[0], TEST_TIME - (3 * ONE_DAY)), null));
        assertFalse(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME - (2 * ONE_DAY)),
                new byte[0], new byte[0], new byte[0], TEST_TIME - (2 * ONE_DAY)), null));
        assertFalse(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME - (1 * ONE_DAY)),
                new byte[0], new byte[0], new byte[0], TEST_TIME - (1 * ONE_DAY)), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME), new byte[0],
                new byte[0], new byte[0], TEST_TIME), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME + ONE_DAY), new byte[0],
                new byte[0], new byte[0], TEST_TIME + ONE_DAY), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.idle", TEST_TIME + (2 * ONE_DAY)),
                new byte[0], new byte[0], new byte[0], TEST_TIME + (2 * ONE_DAY)), null));
        assertFalse(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME - (3 * ONE_DAY)),
                new byte[0], new byte[0], new byte[0], TEST_TIME - (3 * ONE_DAY)), null));
        assertFalse(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME - (2 * ONE_DAY)),
                new byte[0], new byte[0], new byte[0], TEST_TIME - (2 * ONE_DAY)), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME - (1 * ONE_DAY)),
                new byte[0], new byte[0], new byte[0], TEST_TIME - (1 * ONE_DAY)), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME), new byte[0],
                new byte[0], new byte[0], TEST_TIME), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + ONE_DAY), new byte[0],
                new byte[0], new byte[0], TEST_TIME + ONE_DAY), null));
        assertTrue(filter.accept(new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + (2 * ONE_DAY)),
                new byte[0], new byte[0], new byte[0], TEST_TIME + (2 * ONE_DAY)), null));
    }

}

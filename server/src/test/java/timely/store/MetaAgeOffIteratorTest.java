package timely.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;
import timely.adapter.accumulo.MetaAdapter;
import timely.adapter.accumulo.MetricAdapter;

public class MetaAgeOffIteratorTest {

    private static final Long TEST_TIME = System.currentTimeMillis() - 1000;
    private static final Integer ONE_DAY = 86400000;
    private static final Collection<ByteSequence> columnFamilies = new ArrayList<>();

    @Test(expected = IllegalArgumentException.class)
    public void testDefaultMissing() throws Exception {
        SortedMap<Key, Value> table = new TreeMap<>();
        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(table);
        MetaAgeOffIterator iter = new MetaAgeOffIterator();
        HashMap<String, String> options = new HashMap<>();
        iter.init(source, options, null);
    }

    @Test
    public void testDefault() throws Exception {
        SortedMap<Key, Value> table = new TreeMap<>();
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 1), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 2), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 3), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 4), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 5), new Value(new byte[0]));

        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(table);
        MetaAgeOffIterator iter = new MetaAgeOffIterator();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "default", Integer.toString(1 * ONE_DAY));
        iter.init(source, options, null);
        iter.seek(new Range(), columnFamilies, true);
        int seen = 0;
        while (iter.hasTop()) {
            Key k = iter.getTopKey();
            Assert.assertTrue(k.getTimestamp() >= TEST_TIME && k.getTimestamp() <= TEST_TIME + 5);
            seen++;
            iter.next();
        }
        Assert.assertEquals(6, seen);
    }

    @Test
    public void testMixed() throws Exception {
        SortedMap<Key, Value> table = new TreeMap<>();
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME + 1), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME + 2), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME + 3), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME + 4), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME + 5), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 1), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 2), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 3), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 4), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 5), new Value(new byte[0]));

        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(table);
        MetaAgeOffIterator iter = new MetaAgeOffIterator();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "default", Integer.toString(1 * ONE_DAY));
        iter.init(source, options, null);
        iter.seek(new Range(), columnFamilies, true);
        int seen = 0;
        while (iter.hasTop()) {
            Key k = iter.getTopKey();
            Assert.assertTrue(k.getTimestamp() >= TEST_TIME && k.getTimestamp() <= TEST_TIME + 5);
            seen++;
            iter.next();
        }
        Assert.assertEquals(12, seen);
    }

    @Test
    public void testAgeoffMixedMetricKey() throws Exception {
        SortedMap<Key, Value> table = new TreeMap<>();
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME - (3 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME - (2 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME - (1 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME + (1 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.idle", TEST_TIME + (2 * ONE_DAY)), new Value(new byte[0]));

        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME - (3 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME - (2 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME - (1 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + (1 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + (2 * ONE_DAY)), new Value(new byte[0]));

        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(table);
        MetaAgeOffIterator iter = new MetaAgeOffIterator();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "default", Integer.toString(1 * ONE_DAY));
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "sys.cpu.user", Integer.toString(2 * ONE_DAY));
        iter.init(source, options, null);
        iter.seek(new Range(), columnFamilies, true);
        int seen = 0;
        while (iter.hasTop()) {
            Key k = iter.getTopKey();
            Assert.assertTrue(
                    k.getTimestamp() >= (TEST_TIME - (2 * ONE_DAY)) && k.getTimestamp() <= TEST_TIME + (2 * ONE_DAY));
            seen++;
            iter.next();
        }
        Assert.assertEquals(7, seen);

    }

    @Test
    public void testAgeoffMixedTagKey() throws Exception {
        SortedMap<Key, Value> table = new TreeMap<>();
        table.put(MetaAdapter.createTagKey("sys.cpu.idle", "tag1", TEST_TIME - (3 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.idle", "tag2", TEST_TIME - (2 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.idle", "tag3", TEST_TIME - (1 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.idle", "tag4", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.idle", "tag5", TEST_TIME + (1 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.idle", "tag6", TEST_TIME + (2 * ONE_DAY)), new Value(new byte[0]));

        table.put(MetaAdapter.createTagKey("sys.cpu.user", "tag1", TEST_TIME - (3 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.user", "tag2", TEST_TIME - (2 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.user", "tag3", TEST_TIME - (1 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.user", "tag4", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.user", "tag5", TEST_TIME + (1 * ONE_DAY)), new Value(new byte[0]));
        table.put(MetaAdapter.createTagKey("sys.cpu.user", "tag6", TEST_TIME + (2 * ONE_DAY)), new Value(new byte[0]));

        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(table);
        MetaAgeOffIterator iter = new MetaAgeOffIterator();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "default", Integer.toString(1 * ONE_DAY));
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "sys.cpu.user", Integer.toString(2 * ONE_DAY));
        iter.init(source, options, null);
        iter.seek(new Range(), columnFamilies, true);
        int seen = 0;
        while (iter.hasTop()) {
            Key k = iter.getTopKey();
            Assert.assertTrue(
                    k.getTimestamp() >= (TEST_TIME - (2 * ONE_DAY)) && k.getTimestamp() <= TEST_TIME + (2 * ONE_DAY));
            seen++;
            iter.next();
        }
        Assert.assertEquals(7, seen);

    }

    @Test
    public void testAgeoffMixedValueKey() throws Exception {
        SortedMap<Key, Value> table = new TreeMap<>();
        table.put(MetaAdapter.createValueKey("sys.cpu.idle", "tag1", "value1", TEST_TIME - (3 * ONE_DAY)),
                new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.idle", "tag2", "value2", TEST_TIME - (2 * ONE_DAY)),
                new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.idle", "tag3", "value3", TEST_TIME - (1 * ONE_DAY)),
                new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.idle", "tag4", "value4", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.idle", "tag5", "value5", TEST_TIME + (1 * ONE_DAY)),
                new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.idle", "tag6", "value6", TEST_TIME + (2 * ONE_DAY)),
                new Value(new byte[0]));

        table.put(MetaAdapter.createValueKey("sys.cpu.user", "tag1", "value1", TEST_TIME - (3 * ONE_DAY)),
                new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.user", "tag2", "value2", TEST_TIME - (2 * ONE_DAY)),
                new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.user", "tag3", "value3", TEST_TIME - (1 * ONE_DAY)),
                new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.user", "tag4", "value4", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.user", "tag5", "value5", TEST_TIME + (1 * ONE_DAY)),
                new Value(new byte[0]));
        table.put(MetaAdapter.createValueKey("sys.cpu.user", "tag6", "value6", TEST_TIME + (2 * ONE_DAY)),
                new Value(new byte[0]));

        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(table);
        MetaAgeOffIterator iter = new MetaAgeOffIterator();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "default", Integer.toString(1 * ONE_DAY));
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "sys.cpu.user", Integer.toString(2 * ONE_DAY));
        iter.init(source, options, null);
        iter.seek(new Range(), columnFamilies, true);
        int seen = 0;
        while (iter.hasTop()) {
            Key k = iter.getTopKey();
            Assert.assertTrue(
                    k.getTimestamp() >= (TEST_TIME - (2 * ONE_DAY)) && k.getTimestamp() <= TEST_TIME + (2 * ONE_DAY));
            seen++;
            iter.next();
        }
        Assert.assertEquals(7, seen);

    }

    @Test
    public void testSeekPastEndKey() throws Exception {
        SortedMap<Key, Value> table = new TreeMap<>();
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 1), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 2), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 3), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 4), new Value(new byte[0]));
        table.put(MetaAdapter.createMetricKey("sys.cpu.user", TEST_TIME + 5), new Value(new byte[0]));

        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(table);
        MetaAgeOffIterator iter = new MetaAgeOffIterator();
        HashMap<String, String> options = new HashMap<>();
        options.put(MetaAgeOffIterator.AGE_OFF_PREFIX + "default", Integer.toString(1));
        iter.init(source, options, null);
        iter.seek(new Range(new Key("sys.cpu.user"), true,
                new Key(MetricAdapter.encodeRowKey("sys.cpu.user", TEST_TIME + 3), new byte[0], new byte[0],
                        new byte[0], TEST_TIME + 3),
                true), columnFamilies, true);
        int seen = 0;
        while (iter.hasTop()) {
            Key k = iter.getTopKey();
            Assert.assertTrue(k.getTimestamp() >= TEST_TIME && k.getTimestamp() <= TEST_TIME + 5);
            seen++;
            iter.next();
        }
        Assert.assertEquals(0, seen);
    }

}

package timely.store.iterators;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

import timely.api.model.Metric;

public class DataPointsExpansionIteratorTest extends IteratorTestBase {

    @Test
    public void testExpansionSingleValue() throws Exception {

        // Create new Key/Value with a single value
        long timestamp = System.currentTimeMillis();
        final String metric = "sys.cpu.user";
        final byte[] colf = "host=r01n01".getBytes(StandardCharsets.UTF_8);
        final byte[] colq = "rack=r01".getBytes(StandardCharsets.UTF_8);
        final byte[] viz = new byte[0];
        byte[] row = Metric.encodeRowKey(metric, timestamp);
        // Create a byte array with one Double
        ByteBuffer b = ByteBuffer.allocate(Double.BYTES);
        b.putDouble(5.0D);
        Key k = new Key(row, colf, colq, viz, timestamp);
        Value v = new Value(b.array());

        TreeMap<Key, Value> table = new TreeMap<>();
        table.put(k, v);

        SortedMapIterator source = new SortedMapIterator(table);
        DataPointsExpansionIterator iter = new DataPointsExpansionIterator();
        iter.init(source, Collections.emptyMap(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, false);

        Assert.assertTrue(iter.hasTop());
        Assert.assertEquals(k, iter.getTopKey());
        Assert.assertEquals(b.array(), iter.getTopValue().get());
        iter.next();
        Assert.assertFalse(iter.hasTop());
    }

    @Test
    public void testExpansionMany() throws Exception {

        int iterations = 1000;
        final ByteBuffer scratch = ByteBuffer.allocate(iterations * (Long.BYTES + Double.BYTES));

        Map<Key, Double> expected = new TreeMap<>();

        // Construct a Value with 1000 datapoints.
        long timestamp = System.currentTimeMillis();
        long startTimestamp = timestamp;
        final String metric = "sys.cpu.user";
        byte[] startRow = Metric.encodeRowKey(metric, timestamp);
        final byte[] colf = "host=r01n01".getBytes(StandardCharsets.UTF_8);
        final byte[] colq = "rack=r01".getBytes(StandardCharsets.UTF_8);
        final byte[] viz = new byte[0];

        for (double i = 0; i < iterations; i++) {
            scratch.putLong(timestamp);
            scratch.putDouble(i);
            byte[] row = Metric.encodeRowKey(metric, timestamp);
            expected.put(new Key(row, colf, colq, viz, timestamp), i);
            timestamp += 1000;
        }

        // Put one K,V into table with 1000 serialized data points
        TreeMap<Key, Value> table = new TreeMap<>();
        table.put(new Key(startRow, colf, colq, viz, startTimestamp), new Value(scratch.array()));
        Assert.assertEquals(1, table.size());

        SortedMapIterator source = new SortedMapIterator(table);
        DataPointsExpansionIterator iter = new DataPointsExpansionIterator();
        iter.init(source, Collections.emptyMap(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, false);

        ByteBuffer b = ByteBuffer.allocate(Double.BYTES);
        for (int i = 0; i < iterations; i++) {
            Assert.assertTrue(iter.hasTop());
            Assert.assertNotNull(iter.getTopKey());
            Assert.assertNotNull(iter.getTopValue().get());
            Assert.assertTrue(expected.containsKey(iter.getTopKey()));
            b.clear();
            b.put(iter.getTopValue().get());
            b.position(0);
            Assert.assertEquals(expected.get(iter.getTopKey()), b.getDouble(), 0.0D);
            iter.next();
        }
        Assert.assertFalse(iter.hasTop());

    }

    // TODO: Test Expansion of mixed and single values

    // TODO: Add test where data points are not in time order and range end is
    // in the middle of the values.
}

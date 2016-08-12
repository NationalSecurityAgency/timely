package timely.store.iterators;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

import timely.api.model.Metric;

public class DataPointsCompactionIteratorTest extends IteratorTestBase {

    private final ByteBuffer scratch = ByteBuffer.allocate(Double.BYTES);

    @Test
    public void testCompactSingle() throws Exception {

        ByteBuffer expected = ByteBuffer.allocate(1000 * DataPointsExpansionIterator.TIME_VALUE_LENGTH);
        /*
         * Create a table and populate it with 1000 rows, 1 second apart
         */
        TreeMap<Key, Value> table = new TreeMap<>();
        long timestamp = System.currentTimeMillis();
        final String metric = "sys.cpu.user";
        final byte[] colf = "host=r01n01".getBytes(StandardCharsets.UTF_8);
        final byte[] colq = "rack=r01".getBytes(StandardCharsets.UTF_8);
        final byte[] viz = new byte[0];

        for (double i = 0; i < 1000; i++) {
            scratch.clear();
            scratch.putDouble(i);
            timestamp += 1000;
            byte[] row = Metric.encodeRowKey(metric, timestamp);
            table.put(new Key(row, colf, colq, viz, timestamp), new Value(scratch.array()));
        }
        Assert.assertEquals(1000, table.size());

        SortedMapIterator source = new SortedMapIterator(table);
        source.seek(new Range(), EMPTY_COL_FAMS, false);
        while (source.hasTop()) {
            expected.putLong(source.getTopKey().getTimestamp());
            expected.put(source.getTopValue().get());
            source.next();
        }

        source = new SortedMapIterator(table);
        DataPointsCompactionIterator iter = new DataPointsCompactionIterator();
        IteratorSetting is = new IteratorSetting(1, DataPointsCompactionIterator.class);
        is.addOption(TimeWindowCombiner.ALL_OPTION, "true");
        is.addOption(TimeWindowCombiner.WINDOW_SIZE, "1d");
        iter.init(source, is.getOptions(), SCAN_IE);
        iter.seek(new Range(), EMPTY_COL_FAMS, false);

        Assert.assertTrue(iter.hasTop());
        Key topKey = iter.getTopKey();
        Assert.assertNotNull(topKey);
        Value topValue = iter.getTopValue();
        Assert.assertNotNull(topValue);
        Assert.assertArrayEquals(expected.array(), topValue.get());
        iter.next();
        Assert.assertFalse(iter.hasTop());

    }

    // TODO: Test compaction of many values

    // TODO: Test compaction of mixed values
}

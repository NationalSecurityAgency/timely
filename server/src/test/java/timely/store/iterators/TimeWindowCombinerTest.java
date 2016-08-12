package timely.store.iterators;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

import timely.api.model.Metric;

public class TimeWindowCombinerTest extends IteratorTestBase {

    public static class SummingLongTimeCombiner extends TimeWindowCombiner {

        @Override
        public Value reduce(Key key, Iterator<KeyValuePair> iter) {
            final ByteBuffer scratch = ByteBuffer.allocate(Long.BYTES);
            long result = 0L;
            while (iter.hasNext()) {
                KeyValuePair kvp = iter.next();
                Value v = kvp.getValue();
                scratch.clear();
                scratch.put(v.get());
                scratch.position(0);
                result += scratch.getLong();
            }
            scratch.clear();
            scratch.putLong(result);
            return new Value(scratch.array());
        }
    }

    private final ByteBuffer scratch = ByteBuffer.allocate(Long.BYTES);

    @Test
    public void testTimeWindow() throws Exception {

        /*
         * Create a table and populate it with 1000 rows, 1 second apart
         */
        TreeMap<Key, Value> table = new TreeMap<>();
        long timestamp = System.currentTimeMillis();
        final String metric = "sys.cpu.user";
        final byte[] colf = "host=r01n01".getBytes(StandardCharsets.UTF_8);
        final byte[] colq = "rack=r01".getBytes(StandardCharsets.UTF_8);
        final byte[] viz = new byte[0];

        for (long i = 0; i < 1000; i++) {
            scratch.clear();
            scratch.putLong(i);
            timestamp += 1000;
            byte[] row = Metric.encodeRowKey(metric, timestamp);
            table.put(new Key(row, colf, colq, viz, timestamp), new Value(scratch.array()));
        }
        Assert.assertEquals(1000, table.size());

        /*
         * Create the expected set of results by summing the value every 10 rows
         */
        Map<Key, Value> expected = new TreeMap<>();
        int i = 0;
        Key start = null;
        Long aggregate = 0L;
        SortedMapIterator s = new SortedMapIterator(table);
        s.seek(new Range(), EMPTY_COL_FAMS, false);
        while (s.hasTop()) {
            if (null == start) {
                start = s.getTopKey();
            }
            if ((i % 10) == 0) {
                scratch.clear();
                scratch.putLong(aggregate);
                expected.put(start, new Value(scratch.array()));
                aggregate = 0L;
                start = s.getTopKey();
            }
            scratch.clear();
            scratch.put(s.getTopValue().get());
            scratch.position(0);
            aggregate += scratch.getLong();
            i++;
            s.next();
        }
        scratch.clear();
        scratch.putLong(aggregate);
        expected.put(start, new Value(scratch.array()));
        Assert.assertEquals("Expected size is not correct", 100, expected.size());

        /*
         * Set up a scan iterator that will sum the values in 10 seconds windows
         */
        SummingLongTimeCombiner c = new SummingLongTimeCombiner();
        IteratorSetting is = new IteratorSetting(1, SummingLongTimeCombiner.class);
        is.addOption(TimeWindowCombiner.ALL_OPTION, "true");
        is.addOption(TimeWindowCombiner.WINDOW_SIZE, "10s");

        c.validateOptions(is.getOptions());
        c.init(new SortedMapIterator(table), is.getOptions(), SCAN_IE);
        c.seek(new Range(), EMPTY_COL_FAMS, false);

        Map<Key, Value> results = new TreeMap<>();
        while (c.hasTop()) {
            Key k = c.getTopKey();
            Value v = c.getTopValue();
            results.put((Key) k.clone(), new Value(v.get()));
            c.next();
        }
        Assert.assertEquals("Sizes are not equal", expected.size(), results.size());
        Assert.assertEquals(expected, results);
        for (Entry<Key, Value> exp : expected.entrySet()) {
            Value result = results.get(exp.getKey());
            Assert.assertNotNull("result missing for key: " + exp.getKey(), result);
        }
    }

}

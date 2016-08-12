package timely.store.iterators;

import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

import timely.api.model.Metric;

public class LookaheadIteratorTest extends IteratorTestBase {

    @Test
    public void testLookaheadIterator() throws Exception {

        /*
         * Create a table and insert 10 rows each 1 second apart
         */
        TreeMap<Key, Value> table = new TreeMap<>();
        Value emptyValue = new Value(new byte[0]);
        long timestamp = System.currentTimeMillis();
        final String metric = "sys.cpu.user";
        final byte[] colf = "host=r01n01".getBytes(StandardCharsets.UTF_8);
        final byte[] colq = "rack=r01".getBytes(StandardCharsets.UTF_8);
        final byte[] viz = new byte[0];

        for (long i = 0; i < 10; i++) {
            timestamp += 1000;
            byte[] row = Metric.encodeRowKey(metric, timestamp);
            table.put(new Key(row, colf, colq, viz, timestamp), emptyValue);
        }

        /*
         * Build a set of expected keys in the correct order
         */
        Assert.assertEquals(10, table.size());
        Key[] expected = new Key[10];
        int i = 0;
        SortedMapIterator s = new SortedMapIterator(table);
        s.seek(new Range(), EMPTY_COL_FAMS, false);
        while (s.hasTop()) {
            expected[i] = s.getTopKey();
            s.next();
            i++;
        }

        /*
         * Iterate over the table, peeking and calling next
         */
        SortedMapIterator source = new SortedMapIterator(table);
        LookaheadIterator l = new LookaheadIterator(source);
        l.seek(new Range(), EMPTY_COL_FAMS, false);

        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[0].getTimestamp(), l.getTopKey().getTimestamp());

        // Lookahead at the next value
        KeyValuePair lookahead = l.peek();
        Assert.assertEquals(expected[1].getTimestamp(), lookahead.getKey().getTimestamp());

        // Move ahead to the next value, which is really the last value because
        // of the peek.
        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[1].getTimestamp(), l.getTopKey().getTimestamp());
        l.getTopValue(); // must call getTopKey and getTopValue after a peek to
                         // clear the internal state

        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[2].getTimestamp(), l.getTopKey().getTimestamp());

        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[3].getTimestamp(), l.getTopKey().getTimestamp());

        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[4].getTimestamp(), l.getTopKey().getTimestamp());

        lookahead = l.peek();
        Assert.assertEquals(expected[5].getTimestamp(), lookahead.getKey().getTimestamp());

        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[5].getTimestamp(), l.getTopKey().getTimestamp());
        l.getTopValue(); // must call getTopKey and getTopValue after a peek to
                         // clear the internal state

        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[6].getTimestamp(), l.getTopKey().getTimestamp());

        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[7].getTimestamp(), l.getTopKey().getTimestamp());

        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[8].getTimestamp(), l.getTopKey().getTimestamp());

        l.next();
        Assert.assertTrue(l.hasTop());
        Assert.assertEquals(expected[9].getTimestamp(), l.getTopKey().getTimestamp());

        Assert.assertNull(l.peek());
        l.next();
        Assert.assertFalse(l.hasTop());
        Assert.assertNull(l.peek());

    }
}

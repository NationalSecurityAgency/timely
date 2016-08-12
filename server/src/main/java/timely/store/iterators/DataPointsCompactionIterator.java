package timely.store.iterators;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Iterator designed to compact the metric timestamp and value data points into
 * an interleaved timestamp (long) and metric value (double) byte array.
 *
 */
public class DataPointsCompactionIterator extends TimeWindowCombiner {

    @Override
    public Value reduce(Key key, Iterator<KeyValuePair> iter) {
        final AtomicInteger numValues = new AtomicInteger(0);
        // Use a map to re-sort all of the timestamp/values in this time window.
        Map<Long, byte[]> values = new TreeMap<>();
        // Add all of the timestamp and values to the map, keeping track of the
        // number of pairs
        iter.forEachRemaining(kvp -> {
            byte[] tmp = kvp.getValue().get();

            if (tmp.length == Double.BYTES) {
                numValues.incrementAndGet();
            } else if (tmp.length % (DataPointsExpansionIterator.TIME_VALUE_LENGTH) == 0) {
                numValues.addAndGet(tmp.length / DataPointsExpansionIterator.TIME_VALUE_LENGTH);
            } else {
                throw new RuntimeException("Incorrect number of bytes. " + tmp.length + " not a multiple of "
                        + DataPointsExpansionIterator.TIME_VALUE_LENGTH);
            }
            values.put(kvp.getKey().getTimestamp(), tmp);
        });

        // Write out a new combined value.
        ByteBuffer buf = ByteBuffer.allocate(numValues.get() * (DataPointsExpansionIterator.TIME_VALUE_LENGTH));
        values.forEach((k, v) -> {
            buf.putLong(k);
            buf.put(v);
        });
        return new Value(buf.array());
    }

}

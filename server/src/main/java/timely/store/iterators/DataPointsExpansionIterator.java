package timely.store.iterators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.util.Pair;

import timely.api.model.Metric;

/**
 * Accumulo Iterator that expands the compacted Metric timestamps and values at
 * scan time
 *
 */
public class DataPointsExpansionIterator extends WrappingIterator {

    public static final int TIME_VALUE_LENGTH = Long.BYTES + Double.BYTES;

    private ByteBuffer timesAndValues = null;
    private ByteBuffer metricValue = ByteBuffer.allocate(Double.BYTES);
    private Key startKey = null;
    private WritableKey workKey = null;
    private Key k = null;
    private Value v = new Value();
    private Range range = null;

    @Override
    public Key getTopKey() {
        if (null != k) {
            return k;
        }
        return super.getTopKey();
    }

    @Override
    public Value getTopValue() {
        if (null != k) {
            return v;
        }
        return super.getTopValue();
    }

    @Override
    public void next() throws IOException {
        if (null == timesAndValues || timesAndValues.remaining() == 0) {
            super.next();
            if (super.hasTop()) {
                startKey = super.getTopKey();
                workKey.set(startKey);
                timesAndValues = ByteBuffer.wrap(super.getTopValue().get());
                k = decode(startKey, timesAndValues, v);
            } else {
                k = null;
            }
        } else {
            k = decode(startKey, timesAndValues, v);
        }
        while (k != null && v != null && range.afterEndKey(k)) {
            k = decode(startKey, timesAndValues, v);
        }
    }

    private Key decode(Key startKey, ByteBuffer data, Value value) throws IOException {
        Key key = null;
        if (data.remaining() == 0) {
            key = null;
            value = null;
        } else if (data.remaining() == Double.BYTES) {
            // This value only contains a double
            key = workKey;
            value.set(data.array());
            data.position(8); // simulate that we read from the array
        } else if (data.remaining() >= TIME_VALUE_LENGTH) {
            // This should be a Timestamp following by a metric value
            Long timestamp = data.getLong();
            Pair<String, Long> r = Metric.decodeRowKey(workKey.getRow().getBytes());
            workKey.setRow(Metric.encodeRowKey(r.getFirst(), timestamp));
            workKey.setTimestamp(timestamp);
            key = workKey;
            metricValue.clear();
            data.get(metricValue.array()); // Read Double.BYTES from the Value
                                           // byte array
            metricValue.position(0);
            value.set(metricValue.array());
        } else {
            throw new IOException("Incorrect number of bytes remaining. Expected >= " + TIME_VALUE_LENGTH + ", is: "
                    + data.remaining());
        }
        return key;
    }

    @Override
    public boolean hasTop() {
        if (null != k) {
            return true;
        }
        return super.hasTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        this.range = range;
        if (range.getStartKey() != null) {
            while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
                    && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
                next();
            }
            while (hasTop() && range.beforeStartKey(getTopKey())) {
                next();
            }
        }
        if (super.hasTop()) {
            startKey = super.getTopKey();
            workKey = new WritableKey(startKey);
            timesAndValues = ByteBuffer.wrap(super.getTopValue().get());
            k = decode(startKey, timesAndValues, v);
        }
    }

}

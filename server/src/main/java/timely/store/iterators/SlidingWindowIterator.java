package timely.store.iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.adapter.accumulo.MetricAdapter;

public class SlidingWindowIterator extends WrappingIterator {

    private static final Logger LOG = LoggerFactory.getLogger(SlidingWindowIterator.class);

    public static final String FILTER = "sliding.window.filter";

    protected Key topKey = null;
    protected Value topValue = null;
    protected Double[] filters = null;
    protected LinkedList<Pair<Key, Value>> window = new LinkedList<>();
    protected boolean seenLast = false;

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public boolean hasTop() {
        if (seenLast) {
            return false;
        }
        if (topKey != null && topValue != null) {
            return true;
        }
        return false;
    }

    protected void compute() {
        // compute result
        double result = 0D;
        for (int i = 0; i < filters.length; i++) {
            Double d = MetricAdapter.decodeValue(window.get(i).getSecond().get());
            LOG.trace("compute - value: {}", d);
            result += (filters[i] * d);
        }
        LOG.trace("compute - result: {}", result);
        topKey = window.getLast().getFirst();
        topValue = new Value(MetricAdapter.encodeValue(result));
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
        String size = options.getOrDefault(FILTER, null);
        if (null == size) {
            throw new IllegalArgumentException("Window size must be specified.");
        }
        String[] split = size.split(",");
        filters = new Double[split.length];
        for (int i = 0; i < split.length; i++) {
            filters[i] = Double.parseDouble(split[i]);
        }
        LOG.trace("init - filter: {}, seenLast: {}", Arrays.toString(filters), seenLast);
    }

    @Override
    public void next() throws IOException {
        super.next();
        if (super.hasTop()) {
            window.pollFirst();
            window.add(new Pair<Key, Value>(super.getTopKey(), super.getTopValue()));
            compute();
        } else {
            if (!seenLast) {
                seenLast = true;
            }
            window.add(window.getLast());
            compute();
        }
        LOG.trace("next - seenLast: {}", seenLast);
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        topKey = null;
        topValue = null;
        window.clear();
        seenLast = false;
        super.seek(range, columnFamilies, inclusive);
        // Initialize the array with the first value N times
        if (super.hasTop()) {
            for (int i = 0; i < filters.length; i++) {
                window.add(new Pair<Key, Value>(super.getTopKey(), super.getTopValue()));
                if (i != (filters.length - 1)) {
                    super.next();
                }
            }
            compute();
        } else {
            seenLast = true;
        }
        LOG.trace("seek - window: {}, seenLast: {}", window, seenLast);
    }

}

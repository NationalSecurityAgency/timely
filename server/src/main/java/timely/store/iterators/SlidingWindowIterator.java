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

    public static final Logger LOG = LoggerFactory.getLogger(SlidingWindowIterator.class);

    public static final String FILTER = "sliding.window.filter";

    private Key topKey = null;
    private Value topValue = null;
    private Double[] seeds = null;
    private LinkedList<Pair<Key, Value>> window = new LinkedList<>();
    private boolean seenLast = false;

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
        // compute result
        double result = 0D;
        for (int i = 0; i < seeds.length; i++) {
            Double d = MetricAdapter.decodeValue(window.get(i).getSecond().get());
            LOG.trace("hasTop - value: {}", d);
            result += (seeds[i] * d);
        }
        LOG.trace("hasTop - result: {}", result);
        topKey = window.getLast().getFirst();
        topValue = new Value(MetricAdapter.encodeValue(result));
        return true;
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
        seeds = new Double[split.length];
        for (int i = 0; i < split.length; i++) {
            seeds[i] = Double.parseDouble(split[i]);
        }
        LOG.trace("init - filter: {}, seenLast: {}", Arrays.toString(seeds), seenLast);
    }

    @Override
    public void next() throws IOException {
        super.next();
        if (super.hasTop()) {
            window.pollFirst();
            window.add(new Pair<Key, Value>(super.getTopKey(), super.getTopValue()));
        } else {
            if (!seenLast) {
                seenLast = true;
            }
            window.add(window.getLast());
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        topKey = null;
        topValue = null;
        window.clear();
        // Initialize the array with the first value N times
        if (super.hasTop()) {
            for (int i = 0; i < seeds.length; i++) {
                window.add(new Pair<Key, Value>(super.getTopKey(), super.getTopValue()));
            }
        }
        LOG.trace("seek - window: {}", window);
    }

}

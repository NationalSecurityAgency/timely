package timely.store.iterators;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

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
import timely.model.Metric;

/**
 * Iterator that groups time series so that a filter can be applied to the time
 * series. This iterator will ignore time series that do not have enough data
 * points at the start of the query.
 *
 */
public class TimeSeriesGroupingIterator extends WrappingIterator {

    /**
     * Object representing a single time series and its computed value
     *
     */
    private static class TimeSeries extends TreeMap<Key, Double> implements Serializable {

        private static final long serialVersionUID = 1L;
        private int targetSize;
        private transient TimeSeriesGroupingIterator iter;
        private Double answer;

        public TimeSeries(TimeSeriesGroupingIterator iter, int size) {
            this.iter = iter;
            this.targetSize = size;
        }

        @Override
        public Double put(Key key, Double value) {
            return this.put(key, value, true);
        }

        public Double put(Key key, Double value, boolean doCompute) {
            if (this.targetSize == super.size()) {
                Entry<Key, Double> e = this.pollFirstEntry();
                LOG.trace("Removing first entry {}", e.getKey());
            }
            Double result = super.put(key, value);
            if (doCompute) {
                recompute();
            }
            return result;
        }

        public Double getAnswer() {
            return this.answer;
        }

        private void recompute() {
            this.answer = iter.compute(this);
            LOG.trace("recomputed, new answer: {}", this.answer);
        }

    }

    private static class TimeSeriesMetricComparator implements Comparator<Metric>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Metric m1, Metric m2) {
            int result = m1.getName().compareTo(m2.getName());
            if (result == 0) {
                result = m1.getTags().toString().compareTo(m2.getTags().toString());
            }
            return result;
        }

    }

    /**
     *
     * Object representing a group of time series, where uniqueness is defined
     * by the name of the metric and a unique tag set.
     *
     */
    private static class TimeSeriesGroup extends TreeMap<Metric, TimeSeries> implements Iterable<Pair<Key, Double>> {

        private static final long serialVersionUID = 1L;

        public TimeSeriesGroup() {
            super(new TimeSeriesMetricComparator());
        }

        /**
         * Iterates over the time series group and returns for each time series
         * the last key and the filtered value.
         */
        @Override
        public Iterator<Pair<Key, Double>> iterator() {

            final Iterator<Entry<Metric, TimeSeries>> iter = this.entrySet().iterator();

            return new Iterator<Pair<Key, Double>>() {

                @Override
                public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override
                public Pair<Key, Double> next() {
                    Entry<Metric, TimeSeries> e = iter.next();
                    LOG.trace("Removing first entry from series {}", e.getValue());
                    get(e.getKey()).pollFirstEntry(); // remove first key, will
                                                      // re-fill later
                    return new Pair<Key, Double>(e.getValue().lastKey(), e.getValue().getAnswer());
                }

            };
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesGroupingIterator.class);

    public static final String FILTER = "sliding.window.filter";

    private final TimeSeriesGroup series = new TimeSeriesGroup();
    protected Double[] filters = null;
    private Key topKey = null;
    private Value topValue = null;
    private Iterator<Pair<Key, Double>> seriesIterator = null;

    protected Double compute(Map<Key, Double> values) {
        double result = 0D;
        int i = 0;
        for (Entry<Key, Double> e : values.entrySet()) {
            LOG.trace("compute - key:{}, value: {}", e.getKey(), e.getValue());
            result += (filters[i] * e.getValue());
            i++;
        }
        LOG.trace("compute - result: {}", result);
        return result;
    }

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
        LOG.trace("hasTop()");
        if (seriesIterator.hasNext()) {
            LOG.trace("seriesIterator - hasNext()");
            Pair<Key, Double> p = seriesIterator.next();
            topKey = p.getFirst();
            topValue = new Value(MetricAdapter.encodeValue(p.getSecond()));
            return true;
        } else {
            try {
                refillBuffer();
                if (series.size() == 0) {
                    return false;
                } else {
                    seriesIterator = series.iterator();
                    Pair<Key, Double> p = seriesIterator.next();
                    topKey = p.getFirst();
                    topValue = new Value(MetricAdapter.encodeValue(p.getSecond()));
                    return true;
                }
            } catch (IOException e) {
                throw new RuntimeException("Error filling buffer", e);
            }
        }
    }

    @Override
    public void next() throws IOException {
        LOG.trace("next()");
    }

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
        String filterOption = options.getOrDefault(FILTER, null);
        if (null == filterOption) {
            throw new IllegalArgumentException("Window size must be specified.");
        }
        String[] split = filterOption.split(",");
        filters = new Double[split.length];
        for (int i = 0; i < split.length; i++) {
            filters[i] = Double.parseDouble(split[i]);
        }
        LOG.trace("init - filter: {}", Arrays.toString(filters));
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        super.seek(range, columnFamilies, inclusive);
        series.clear();
        fillBuffer();
        seriesIterator = series.iterator();
    }

    private void refillBuffer() throws IOException {
        LOG.trace("refill()");
        Metric top = null;
        while (super.hasTop()) {
            Key k = super.getTopKey();
            Metric m = MetricAdapter.parse(k, super.getTopValue());
            timely.model.Value v = m.getValue();
            m.setValue(null);
            Collections.sort(m.getTags());
            if (top != null && top.equals(m)) {
                // we have cycled back to the first series
                break;
            }
            if (top == null) {
                top = m;
            }
            TimeSeries values = series.get(m);
            // new time series found, ignore it
            if (null != values) {
                LOG.trace("Adding value {} to series {}", v.getMeasure(), m);
                values.put(k, v.getMeasure(), true);
            } else {
                LOG.trace("Ignoring new time series {}", m);
            }
            super.next();
        }
        removeSeriesWithMissingValues();
    }

    private void fillBuffer() throws IOException {
        // fill buffer
        Metric top = null;
        int seenTop = 0;
        while (super.hasTop()) {
            Key k = super.getTopKey();
            Metric m = MetricAdapter.parse(k, super.getTopValue());
            timely.model.Value v = m.getValue();
            m.setValue(null);
            Collections.sort(m.getTags());
            if (null == top) {
                top = m;
            }
            if (top.equals(m)) {
                seenTop++;
            }
            if (seenTop == (filters.length + 1)) {
                break;
            }
            TimeSeries values = series.getOrDefault(m, new TimeSeries(this, filters.length));
            series.putIfAbsent(m, values);
            if (values.size() < filters.length) {
                LOG.trace("Adding value {} to series {}", v.getMeasure(), m);
                values.put(k, v.getMeasure(), false);
            }
            super.next();
        }
        removeSeriesWithMissingValues();
        computeAllSeries();
    }

    private void computeAllSeries() {
        series.values().forEach(v -> v.recompute());
    }

    private void removeSeriesWithMissingValues() {
        // Remove series that don't have enough data points
        Set<Metric> keysToRemove = new HashSet<>();
        Iterator<Entry<Metric, TimeSeries>> e = series.entrySet().iterator();
        while (e.hasNext()) {
            Entry<Metric, TimeSeries> entry = e.next();
            if (entry.getValue().size() != filters.length) {
                keysToRemove.add(entry.getKey());
            }
        }
        keysToRemove.forEach(k -> {
            LOG.trace("Removing entry because not enough data points for filter: " + k);
            series.remove(k);

        });
    }

}

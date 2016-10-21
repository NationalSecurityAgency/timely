package timely.store.iterators;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
 * NOTE: This iterator does not handle being re-seeked. It is currently designed
 * to be used with the DownsampleIterator above it.
 *
 */
public class TimeSeriesGroupingIterator extends WrappingIterator {

    /**
     * Object representing a single time series and its computed value
     *
     */
    private static class TimeSeries extends LinkedList<Pair<Key, Double>> implements Serializable {

        private static final long serialVersionUID = 1L;
        private int targetSize;
        private transient TimeSeriesGroupingIterator iter;
        private Double answer;

        public TimeSeries(TimeSeriesGroupingIterator iter, int size) {
            this.iter = iter;
            this.targetSize = size;
        }

        public void add(Key key, Double value) {
            if (this.targetSize == super.size()) {
                Pair<Key, Double> e = this.pollFirst();
                LOG.trace("Removing first entry {}", e.getFirst());
            }
            super.add(new Pair<>(key, value));
            if (super.size() < this.targetSize) {
                this.answer = null;
            } else {
                recompute();
            }
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

                private Pair<Key, Double> next = findNext();

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                private Pair<Key, Double> findNext() {
                    Entry<Metric, TimeSeries> e = null;
                    while (iter.hasNext()) {
                        e = iter.next();
                        if (e.getValue().getAnswer() != null) {
                            break;
                        }
                    }

                    if (null != e && e.getValue().getAnswer() != null) {
                        LOG.trace("Removing first entry from series {}", e.getValue());
                        // remove first key, will re-fill later
                        get(e.getKey()).pollFirst();
                        return new Pair<Key, Double>(e.getValue().getLast().getFirst(), e.getValue().getAnswer());
                    } else {
                        return null;
                    }
                }

                @Override
                public Pair<Key, Double> next() {
                    try {
                        LOG.trace("Returning {}", next);
                        return next;
                    } finally {
                        next = findNext();
                    }
                }

            };
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesGroupingIterator.class);

    public static final String FILTER = "sliding.window.filter";
    private static final TimeSeriesMetricComparator metricComparator = new TimeSeriesMetricComparator();

    private TimeSeriesGroup series = new TimeSeriesGroup();
    protected Double[] filters = null;
    private Key topKey = null;
    private Value topValue = null;
    private Iterator<Pair<Key, Double>> seriesIterator = null;

    protected Double compute(List<Pair<Key, Double>> values) {
        double result = 0D;
        int i = 0;
        for (Pair<Key, Double> e : values) {
            LOG.trace("compute - key:{}, value: {}", e.getFirst(), e.getSecond());
            result += (filters[i] * e.getSecond());
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
        return (null != topKey && null != topValue);
    }

    private void setTopKeyValue() {
        if (!seriesIterator.hasNext()) {
            do {
                try {
                    refillBuffer();
                    seriesIterator = series.iterator();
                } catch (IOException e) {
                    throw new RuntimeException("Error filling buffer", e);
                }
            } while (!seriesIterator.hasNext() && super.hasTop());

            if (series.size() == 0) {
                seriesIterator = null;
                topKey = null;
                topValue = null;
            }
        }
        if (null != seriesIterator && seriesIterator.hasNext()) {
            Pair<Key, Double> p = seriesIterator.next();
            topKey = p.getFirst();
            topValue = new Value(MetricAdapter.encodeValue(p.getSecond()));
        }
    }

    @Override
    public void next() throws IOException {
        LOG.trace("next()");
        setTopKeyValue();
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
        seriesIterator = series.iterator();
        setTopKeyValue();
    }

    private void refillBuffer() throws IOException {
        LOG.trace("refill()");
        TimeSeriesGroup nextSeries = new TimeSeriesGroup();
        Metric prev = null;
        while (super.hasTop()) {
            Key k = super.getTopKey();
            Metric m = MetricAdapter.parse(k, super.getTopValue());
            timely.model.Value v = m.getValue();
            m.setValue(null);
            Collections.sort(m.getTags());
            if (prev != null && metricComparator.compare(prev, m) >= 0) {
                // Only process metrics that sort after the previous one.
                break;
            }
            prev = m;
            TimeSeries values = series.remove(m);
            if (null == values) {
                LOG.trace("Creating new time series {}", m);
                values = new TimeSeries(this, filters.length);
            }
            LOG.trace("Adding value {} to series {}", v.getMeasure(), m);
            values.add(k, v.getMeasure());
            nextSeries.put(m, values);
            super.next();
        }
        if (!nextSeries.isEmpty()) {
            this.series.forEach((m, v) -> {
                Pair<Key, Double> last = v.getLast();
                v.add(last.getFirst(), last.getSecond());
                LOG.trace("Adding value {} to series {}", v, m);
                nextSeries.put(m, v);
            });
        }
        this.series = nextSeries;
        LOG.trace("New Buffer: {}", series);
    }

}

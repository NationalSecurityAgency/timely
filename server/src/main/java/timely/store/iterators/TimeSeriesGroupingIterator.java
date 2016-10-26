package timely.store.iterators;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.adapter.accumulo.MetricAdapter;
import timely.model.Metric;
import timely.model.Tag;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

/**
 * Iterator that groups time series so that a filter can be applied to the time
 * series. Filters are specified as a comma separated list of doubles. For
 * example, a five day moving average filter would be specified as:
 * "0.20,0.20,0.20,0.20,0.20"
 *
 * Note that this iterator will consume at the start F keys where F is the
 * number of elements in the filter. So, if you have a five day moving average
 * filter and a time series as 100 data points, then you will receive 95
 * results.
 * 
 * For multiple time series, K/V pairs will be reported in time order where time
 * is taken from the key of the last element in the filter. For example, if you
 * have series {A,B,C} and {A,C} are on the same time interval and B is lagging
 * behind them, then this iterator will return K/V answers for the time series
 * in the following manner:
 * 
 * A, C, B, A, C, B, ...
 * 
 * Also of note is that this iterator handles new time series appearing in the
 * middle of the time range. Time series that disappear during the time range
 * will stop reporting an answer.
 * 
 * NOTE: This iterator does not handle being re-seeked. It is currently designed
 * to be used with the DownsampleIterator above it.
 *
 */
public class TimeSeriesGroupingIterator extends WrappingIterator {

    /**
     * Object representing a single time series and its computed value. The
     * value will be computed when the time series its values reach the target
     * size. Calling getAndRemoveAnswer will return the answer and clear it's
     * internal representation.
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
            super.add(new Pair<>(key, value));
            if (super.size() == this.targetSize) {
                recompute();
                // remove first key as it is no longer needed for any
                // computations
                Pair<Key, Double> e = this.pollFirst();
                LOG.trace("Removing first entry {}", e.getFirst());
            }
        }

        public Double getAndRemoveAnswer() {
            try {
                return this.answer;
            } finally {
                this.answer = null;
            }
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
                List<Tag> m1Tags = m1.getTags();
                List<Tag> m2Tags = m2.getTags();
                int size = Math.min(m1Tags.size(), m2Tags.size());
                for (int i = 0; result == 0 && i < size; i++) {
                    result = m1Tags.get(i).compareTo(m2Tags.get(i));
                }
                if (result == 0) {
                    result = Integer.compare(m1Tags.size(), m2Tags.size());
                }
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
                    Double answer = null;
                    while (iter.hasNext()) {
                        e = iter.next();
                        answer = e.getValue().getAndRemoveAnswer();
                        if (answer != null) {
                            break;
                        }
                    }

                    if (answer != null) {
                        return new Pair<Key, Double>(e.getValue().getLast().getFirst(), answer);
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
        while (!seriesIterator.hasNext() && super.hasTop()) {
            try {
                refillBuffer();
                seriesIterator = series.iterator();
            } catch (IOException e) {
                throw new RuntimeException("Error filling buffer", e);
            }
        }

        if (seriesIterator.hasNext()) {
            Pair<Key, Double> p = seriesIterator.next();
            topKey = p.getFirst();
            topValue = new Value(MetricAdapter.encodeValue(p.getSecond()));
        } else {
            topKey = null;
            topValue = null;
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
        String[] split = StringUtils.split(filterOption, ',');
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
            TimeSeries values = series.get(m);
            if (null == values) {
                LOG.trace("Creating new time series {}", m);
                values = new TimeSeries(this, filters.length);
                series.put(m, values);
            }
            LOG.trace("Adding value {} to series {}", v.getMeasure(), m);
            values.add(k, v.getMeasure());
            super.next();
        }
        LOG.trace("Buffer contents: {}", series);
    }

}

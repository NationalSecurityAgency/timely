package timely.sample;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.ToStringBuilder;

import timely.api.request.timeseries.QueryRequest.RateOption;

import com.google.common.base.Preconditions;

public class Downsample implements Iterable<Sample>, Serializable {

    public static class AggregatedValue implements Serializable {

        private static final long serialVersionUID = 1L;

        private int count = 0;
        private double value = 0.0D;

        public void incrementCount() {
            count++;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public double getValue() {
            return value;
        }

        public void setValue(double value) {
            this.value = value;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("{count: ").append(this.count);
            buf.append(" value: ").append(this.value).append("}");
            return buf.toString();
        }

    }

    private static final long serialVersionUID = 1L;

    private transient Aggregator aggregator;
    private final String aggregatorClassname;
    private final long start;
    private final long period;

    private final TreeMap<Long, AggregatedValue> buckets = new TreeMap<>();

    public Downsample(long start, long end, long period, Aggregator agg) {
        Preconditions.checkArgument(start < end, "Start must be < end");
        Preconditions.checkArgument(period >= 1, "period cannot be < 1");
        Preconditions.checkNotNull(agg, "Aggregator object cannot be null");
        this.start = start;
        this.period = period;
        this.aggregator = agg;
        this.aggregatorClassname = agg.getClass().getName();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        try {
            aggregator = (Aggregator) getClass().getClassLoader().loadClass(aggregatorClassname).newInstance();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void add(long ts, double value) {
        if (ts < start) {
            throw new IllegalArgumentException("timestamp is out of range");
        }
        // find the start time of the bucket
        long sampleStart = ts - ((ts - start) % period);
        AggregatedValue val = buckets.get(sampleStart);
        if (null == val) {
            buckets.put(sampleStart, val = new AggregatedValue());
        }
        val.setValue(aggregator.aggregate(val.getValue(), val.getCount(), value));
        val.incrementCount();
    }

    public void merge(Downsample other) {
        for (Entry<Long, AggregatedValue> e : other.buckets.entrySet()) {
            AggregatedValue thisVal = buckets.get(e.getKey());
            if (null == thisVal) {
                buckets.put(e.getKey(), e.getValue());
            } else {
                thisVal.setCount(thisVal.getCount() + e.getValue().getCount());
                thisVal.setValue(aggregator.aggregate(thisVal.getValue(), thisVal.getCount(), e.getValue().getValue()));
            }
        }
    }

    @Override
    public Iterator<Sample> iterator() {
        return new Iterator<Sample>() {

            Iterator<Long> keys = buckets.keySet().iterator();

            @Override
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public Sample next() {
                long time = keys.next();
                AggregatedValue val = buckets.get(time);
                double value = aggregator.last(val.getValue(), val.getCount());
                return new Sample(time, value);
            }
        };
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("aggregator", this.aggregatorClassname);
        tsb.append("period", this.period);
        tsb.append("start", this.start);
        tsb.append("values={");
        for (Entry<Long, AggregatedValue> e : this.buckets.entrySet()) {
            tsb.append(e.getKey()).append("=").append(e.getValue()).append(", ");
        }
        tsb.append("}");
        return tsb.toString();
    }

    public static Downsample combine(Collection<Downsample> samples, RateOption rateOption) {
        List<Downsample> sampleList = new ArrayList<>(samples.size());
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("Empty samples to combine");
        }
        for (Downsample ds : samples) {
            sampleList.add(ds);
        }
        Iterator<Downsample> iter = sampleList.iterator();
        Downsample result = iter.next();
        while (iter.hasNext()) {
            result.merge(iter.next());
        }
        return result;
    }

}

package timely.sample;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.ToStringBuilder;
import timely.api.request.timeseries.QueryRequest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.*;

public class Aggregation implements Iterable<Sample>, Serializable {

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

    protected transient Aggregator aggregator;
    protected final String aggregatorClassname;

    protected final TreeMap<Long, AggregatedValue> buckets = new TreeMap<>();

    public Aggregation(Aggregator agg) {
        Preconditions.checkNotNull(agg, "Aggregator object cannot be null");
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
        AggregatedValue val = buckets.get(ts);
        if (null == val) {
            buckets.put(ts, val = new AggregatedValue());
        }
        val.setValue(aggregator.aggregate(val.getValue(), val.getCount(), value));
        val.incrementCount();
    }

    public void merge(Aggregation other) {
        for (Map.Entry<Long, AggregatedValue> e : other.buckets.entrySet()) {
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
        tsb.append("values={");
        for (Map.Entry<Long, AggregatedValue> e : this.buckets.entrySet()) {
            tsb.append(e.getKey()).append("=").append(e.getValue()).append(", ");
        }
        tsb.append("}");
        return tsb.toString();
    }

    public static Aggregation combineAggregation(Collection<Aggregation> samples, QueryRequest.RateOption rateOption) {
        List<Aggregation> sampleList = new ArrayList<>(samples.size());
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("Empty samples to combine");
        }
        for (Aggregation a : samples) {
            sampleList.add(a);
        }
        Iterator<Aggregation> iter = sampleList.iterator();
        Aggregation result = iter.next();
        while (iter.hasNext()) {
            result.merge(iter.next());
        }
        return result;
    }

}

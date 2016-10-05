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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    }

    private static Logger LOG = LoggerFactory.getLogger(Downsample.class);

    private static final long serialVersionUID = 1L;

    private transient Aggregator aggregator;
    private final String aggregatorClassname;
    private final long start;
    private final long period;
    // private final double[] values;
    // private final int[] counts;

    private final TreeMap<Long, AggregatedValue> buckets = new TreeMap<>();

    public Downsample(long start, long end, long period, Aggregator agg) {
        Preconditions.checkArgument(start < end, "Start must be < end");
        Preconditions.checkArgument(period >= 1, "period cannot be < 1");
        Preconditions.checkNotNull(agg, "Aggregator object cannot be null");
        this.start = start;
        this.period = period;
        this.aggregator = agg;
        this.aggregatorClassname = agg.getClass().getName();
        long buckets = (end - start) / period + 1;
        if (buckets > Integer.MAX_VALUE) {
            throw new RuntimeException("Cannot downsample to " + buckets + " items");
        }
        // values = new double[(int) buckets];
        // counts = new int[(int) buckets];
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
        tsb.append("values", this.buckets);
        return tsb.toString();
    }

    public static Downsample combine(Collection<Downsample> samples, RateOption rateOption) {
        List<Downsample> sampleList = new ArrayList<>(samples.size());
        if (samples.isEmpty()) {
            throw new IllegalArgumentException("Empty samples to combine");
        }
        for (Downsample ds : samples) {
            // if (rateOption != null && rateOption.isCounter()) {
            // ds = convertCountToRate(ds, rateOption.getResetValue(),
            // rateOption.getCounterMax());
            // }
            // ds = interpolateMissingValues(ds);
            sampleList.add(ds);
        }
        Iterator<Downsample> iter = sampleList.iterator();
        Downsample result = iter.next();
        while (iter.hasNext()) {
            result.merge(iter.next());
        }
        return result;
    }

    // private static Downsample interpolateMissingValues(Downsample ds) {
    // Downsample result = new Downsample(ds.start, ds.start + ds.counts.length
    // * ds.period, ds.period, ds.aggregator);
    // int lastPos = -1;
    // for (int i = 0; i < ds.counts.length; i++) {
    // if (ds.counts[i] != 0) {
    // result.values[i] = ds.values[i];
    // result.counts[i] = ds.counts[i];
    // if (lastPos >= 0) {
    // double diff = ds.values[i] - ds.values[lastPos];
    // for (int j = lastPos + 1; j < i; j++) {
    // result.values[j] = result.values[j - 1] + diff / (i - lastPos);
    // result.counts[j] = 1;
    // }
    // }
    // lastPos = i;
    // }
    // }
    // LOG.debug("interpolate: {}", result);
    // return result;
    // }

    // private static Downsample convertCountToRate(Downsample ds, long
    // resetValue, long counterMax) {
    // Downsample result = new Downsample(ds.start + ds.period, ds.start +
    // ds.counts.length * ds.period, ds.period,
    // ds.aggregator);
    // int lastPos = 0;
    // for (int i = 1; i < ds.counts.length; i++) {
    // if (ds.counts[i] != 0) {
    // if (((Long) counterMax).equals(((Double) ds.values[i]).longValue())) {
    // result.values[i - 1] = resetValue;
    // result.counts[i - 1] = 1;
    // } else {
    // double diff = ds.values[i] - ds.values[lastPos];
    // if (diff < 0) {
    // // wrap around formula from
    // // https://collectd.org/wiki/index.php/Data_source says
    // // to do
    // // the following:
    // // double wrapValue = (counterMax - ds.values[lastPos] +
    // // ds.values[i]) / (ds.period);
    // // When counterMax is not defined it defaults to
    // // Long.MAX_VALUE which leaves wrapValue
    // // very close to counterMax. This renders the resulting
    // // graph useless as it blows up the
    // // scale, we are going to set the value to -1. If we
    // // have a constantly decreasing counter,
    // // which in theory should not happen, then all of the
    // // values will be -1.
    // double wrapValue = -1.0;
    // result.values[i - 1] = wrapValue;
    // result.counts[i - 1] = 1;
    // } else {
    // result.values[i - 1] = diff;
    // result.counts[i - 1] = 1;
    // }
    // }
    // lastPos = i;
    // }
    // }
    // LOG.debug("convertCountToRate: {}", result);
    // return result;
    // }
}

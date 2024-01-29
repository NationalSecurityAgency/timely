package timely.sample;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.google.common.base.Preconditions;

import timely.api.request.timeseries.QueryRequest.RateOption;

public class Downsample extends Aggregation implements Iterable<Sample>, Serializable {

    private static final long serialVersionUID = 1L;

    protected final long start;
    protected final long period;

    public Downsample(long start, long end, long period, Aggregator agg) {
        super(agg);
        Preconditions.checkArgument(start < end, "Start must be < end");
        Preconditions.checkArgument(period >= 1, "period cannot be < 1");
        this.start = start;
        this.period = period;
    }

    @Override
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

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("period", this.period);
        tsb.append("start", this.start);
        tsb.append(super.toString());
        return tsb.toString();
    }

    public static Downsample combineDownsample(Collection<Downsample> samples, RateOption rateOption) {
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

package timely.sample.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.timeseries.QueryRequest;
import timely.model.Metric;
import timely.model.ObjectSizeOf;
import timely.model.Tag;
import timely.api.response.TimelyException;
import timely.sample.Aggregator;
import timely.sample.Downsample;
import timely.sample.DownsampleFactory;
import timely.sample.aggregators.Avg;

import static org.apache.accumulo.core.conf.AccumuloConfiguration.getTimeInMillis;

public class DownsampleIterator extends WrappingIterator {

    public static final String START = "downsample.start";
    public static final String END = "downsample.end";
    public static final String PERIOD = "downsample.period";
    public static final String MAX_DOWNSAMPLE_MEMORY = "downsample.maxDownsampleMemory";
    public static final String AGGCLASS = "downsample.aggclass";

    private static final long DEFAULT_DOWNSAMPLE_MS = 1;
    private static final String DEFAULT_DOWNSAMPLE_AGGREGATOR = Avg.class.getSimpleName().toLowerCase();

    private DownsampleFactory factory;
    private final Map<Set<Tag>, Downsample> value = new HashMap<>();
    private long start;
    private long end;
    private long period;
    private Key last;
    private MemoryEstimator memoryEstimator = null;

    static public class MemoryEstimator {

        private boolean newBucket = false;
        private long start;
        private long period;
        private long startOfCurrentBucket;
        private long bucketsCompleted = 0;
        private long bytesPerBucket = 0;
        boolean highVolumeBuckets = false;
        long maxDownsampleMemory = 0; // max aggregation cache (bytes) before
                                      // current batch is returned (after
                                      // bucket is complete)
        LinkedList<Long> percentageChecks = new LinkedList<>();

        public void reset() {
            newBucket = false;
            bucketsCompleted = 0;
            bytesPerBucket = 0;
            percentageChecks.add(5l);
            percentageChecks.add(25l);
            percentageChecks.add(50l);
            percentageChecks.add(75l);
        }

        public MemoryEstimator(long maxDownsampleMemory, long start, long period) {
            this.maxDownsampleMemory = maxDownsampleMemory;
            this.start = start;
            this.period = period;
            this.startOfCurrentBucket = this.start;
            reset();
        }

        public double getMemoryUsedPercentage() {
            return (bucketsCompleted * bytesPerBucket) / (double) maxDownsampleMemory * 100;
        }

        public long getBucketsCompleted() {
            return bucketsCompleted;
        }

        public long getBytesPerBucket() {
            return bytesPerBucket;
        }

        public boolean isHighVolumeBuckets() {
            return highVolumeBuckets;
        }

        public boolean shouldReturnBasedOnMemoryUsage(long timestamp, Object value) {

            boolean shouldReturn = false;
            if (maxDownsampleMemory >= 0) {
                sample(timestamp);
                if (isNewBucket()) {
                    bucketsCompleted++;
                    double memoryUsedPercentage = getMemoryUsedPercentage();
                    if (memoryUsedPercentage >= 100) {
                        shouldReturn = true;
                    } else {
                        boolean checkMemoryNow = false;
                        Long check = percentageChecks.peek();
                        if (check != null && memoryUsedPercentage >= check) {
                            checkMemoryNow = true;
                            percentageChecks.removeFirst();
                        }
                        // recalculate bytesPerBucket
                        if (bytesPerBucket == 0 || highVolumeBuckets || checkMemoryNow) {
                            long memoryUsed = ObjectSizeOf.Sizer.getObjectSize(value);
                            bytesPerBucket = memoryUsed / bucketsCompleted;
                        }
                        // bucket average greater than 10% of max
                        highVolumeBuckets = (bytesPerBucket / (double) maxDownsampleMemory) >= 0.1;
                        shouldReturn = false;
                    }
                }
            }
            return shouldReturn;
        }

        public boolean isNewBucket() {
            return newBucket;
        }

        private void sample(long timestamp) {
            if (timestamp >= (startOfCurrentBucket + period)) {
                newBucket = true;
                startOfCurrentBucket = timestamp - ((timestamp - start) % period);
            } else {
                newBucket = false;
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
        start = Long.parseLong(options.get(START));
        end = Long.parseLong(options.get(END));
        period = Long.parseLong(options.get(PERIOD));
        // default = 100 MB
        long maxDownsampleMemory = -1;
        if (options.containsKey(MAX_DOWNSAMPLE_MEMORY)) {
            maxDownsampleMemory = Long.parseLong(options.get(MAX_DOWNSAMPLE_MEMORY));
        }
        memoryEstimator = new MemoryEstimator(maxDownsampleMemory, start, period);

        String aggClassname = options.get(AGGCLASS);
        Class<?> aggClass;
        try {
            aggClass = this.getClass().getClassLoader().loadClass(aggClassname);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        factory = new DownsampleFactory(start, end, period, (Class<? extends Aggregator>) aggClass);
    }

    @Override
    public boolean hasTop() {

        if (super.hasTop()) {
            while (super.hasTop()) {
                Key topKey = super.getTopKey();
                Value topValue = super.getTopValue();
                Metric metric = MetricAdapter.parse(topKey, topValue);
                long timestamp = metric.getValue().getTimestamp();
                if (memoryEstimator.shouldReturnBasedOnMemoryUsage(timestamp, value)) {
                    break;
                }
                last = topKey;

                Set<Tag> tags = new HashSet<Tag>(metric.getTags());
                Downsample sample = value.get(tags);
                if (sample == null) {
                    try {
                        value.put(tags, sample = factory.create());
                    } catch (TimelyException e) {
                        throw new RuntimeException(e);
                    }
                }
                sample.add(metric.getValue().getTimestamp(), metric.getValue().getMeasure());
                try {
                    super.next();
                } catch (IOException e) {
                    throw new RuntimeException("Downstream next() failed", e);
                }
            }
            return last != null;
        } else {
            return false;
        }
    }

    @Override
    public Key getTopKey() {
        return last;
    }

    @Override
    public Value getTopValue() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(value);
            out.flush();
            // empty for next batch of downsamples
            value.clear();
            memoryEstimator.reset();
            return new Value(bos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void next() throws IOException {
        if (!super.hasTop()) {
            last = null;
        }
    }

    public static void setDownsampleOptions(IteratorSetting is, long start, long end, long period,
            long maxDownsampleMemory, String classname) {
        is.addOption(START, "" + start);
        is.addOption(END, "" + end);
        is.addOption(PERIOD, "" + period);
        is.addOption(MAX_DOWNSAMPLE_MEMORY, "" + maxDownsampleMemory);
        is.addOption(AGGCLASS, classname);
    }

    @SuppressWarnings("unchecked")
    public static Map<Set<Tag>, Downsample> decodeValue(Value value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value.get());
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (Map<Set<Tag>, Downsample>) ois.readObject();
    }

    public static long getDownsamplePeriod(QueryRequest.SubQuery query) {
        // disabling the downsampling OR setting the aggregation to none are
        // both considered to be disabling
        if (!query.getDownsample().isPresent() || query.getDownsample().get().endsWith("-none")) {
            return DEFAULT_DOWNSAMPLE_MS;
        }
        String parts[] = query.getDownsample().get().split("-");
        return getTimeInMillis(parts[0]);
    }

    public static Class<? extends Aggregator> getDownsampleAggregator(QueryRequest.SubQuery query) {
        String aggregatorName = Aggregator.NONE;
        if (query.getDownsample().isPresent()) {
            String parts[] = query.getDownsample().get().split("-");
            aggregatorName = parts[1];
        }
        // disabling the downsampling OR setting the aggregation to none are
        // both considered to be disabling
        if (aggregatorName.equals(Aggregator.NONE)) {
            // we need a downsampling iterator, so default to max to ensure we
            // return something
            aggregatorName = DEFAULT_DOWNSAMPLE_AGGREGATOR;
        }
        return Aggregator.getAggregator(aggregatorName);
    }
}

package timely.sample.iterators;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

import timely.adapter.accumulo.MetricAdapter;
import timely.model.Metric;
import timely.model.ObjectSizeOf;
import timely.model.Tag;
import timely.api.response.TimelyException;
import timely.sample.Aggregator;
import timely.sample.Downsample;
import timely.sample.DownsampleFactory;

public class DownsampleIterator extends WrappingIterator {

    private static final String START = "downsample.start";
    private static final String END = "downsample.end";
    private static final String PERIOD = "downsample.period";
    private static final String MAX_AGGREGATION_MEMORY = "downsample.maxAggregationMemory";
    private static final String AGGCLASS = "downsample.aggclass";

    private DownsampleFactory factory;
    private final Map<Set<Tag>, Downsample> value = new HashMap<>();
    private long start;
    private long end;
    private long period;
    private Key last;
    private long maxAggregationMemory = 1000; // max aggregation memory (bytes) before current batch is returned (after bucket is complete)
    private boolean doneWithInitialBucketTimestamp = false;

    public class MemoryEstimator {
        private long bucketsCompleted = 0;
        private long bytesPerBucket = 0;
        boolean highVolumeBuckets = false;

        public void reset() {
            bucketsCompleted = 0;
            bytesPerBucket = 0;
        }

        public boolean shouldReturnBasedOnMemoryUsage() {
            bucketsCompleted++;
            double memoryRemainingPercentage = (maxAggregationMemory - (bucketsCompleted * bytesPerBucket)) / maxAggregationMemory * 100;
            if (memoryRemainingPercentage <= 0) {
                return true;
            } else {
                // recalculate bytesPerBucket
                if (bytesPerBucket == 0 || highVolumeBuckets || bucketsCompleted % 100 == 0 || memoryRemainingPercentage < 10.0) {
                    long memoryUsed = ObjectSizeOf.Sizer.getObjectSize(value);
                    bytesPerBucket = memoryUsed / bucketsCompleted;
                }
                highVolumeBuckets = (bytesPerBucket / maxAggregationMemory) > 0.05;
                return false;
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
        if (options.containsKey(MAX_AGGREGATION_MEMORY)) {
            maxAggregationMemory = Long.parseLong(options.get(MAX_AGGREGATION_MEMORY));
        }
        String aggClassname = options.get(AGGCLASS);
        Class<?> aggClass;
        try {
            aggClass = this.getClass().getClassLoader().loadClass(aggClassname);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        factory = new DownsampleFactory(start, end, period, (Class<? extends Aggregator>) aggClass);
    }

    protected boolean beginNewDownsampleBucket(long timestamp) {
        boolean startOfBucket = (timestamp - start) % period == 0;
        if (doneWithInitialBucketTimestamp && startOfBucket) {
            doneWithInitialBucketTimestamp = false;
            return true;
        }
        if (!startOfBucket) {
            doneWithInitialBucketTimestamp = true;
        }
        return false;
    }



    @Override
    public boolean hasTop() {

        if (super.hasTop()) {
            while (super.hasTop()) {
                Key topKey = super.getTopKey();
                Value topValue = super.getTopValue();
                Metric metric = MetricAdapter.parse(topKey, topValue);
                long timestamp = metric.getValue().getTimestamp();
                if (beginNewDownsampleBucket(timestamp)) {
                    bucketsCompleted++;
                    if (bucketsUntilReturn == -1) {
                        long memoryUsed = ObjectSizeOf.Sizer.getObjectSize(value);
                        long memoryPerBucket = memoryUsed / bucketsCompleted;
                        bucketsUntilReturn = (maxAggregationMemory / memoryPerBucket);
                    }
                    bucketsUntilReturn--;

                    // start of new downsample bucket
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

    public static void setDownsampleOptions(IteratorSetting is, long start, long end, long period, String classname) {
        is.addOption(START, "" + start);
        is.addOption(END, "" + end);
        is.addOption(PERIOD, "" + period);
        is.addOption(AGGCLASS, classname);
    }

    public static void setMaxTagSets(IteratorSetting is, long maxTagSets) {
        is.addOption(MAXTAGSETS, "" + maxTagSets);
    }

    @SuppressWarnings("unchecked")
    public static Map<Set<Tag>, Downsample> decodeValue(Value value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value.get());
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (Map<Set<Tag>, Downsample>) ois.readObject();
    }
}

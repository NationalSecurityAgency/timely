package timely.sample.iterators;

import static org.apache.accumulo.core.conf.ConfigurationTypeHelper.getTimeInMillis;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.model.Metric;
import timely.model.Tag;
import timely.sample.Aggregator;
import timely.sample.Downsample;
import timely.sample.DownsampleFactory;
import timely.sample.aggregators.Avg;

public class DownsampleIterator extends WrappingIterator {

    private static final Logger LOG = LoggerFactory.getLogger(DownsampleIterator.class);
    private static final String START = "downsample.start";
    private static final String END = "downsample.end";
    private static final String PERIOD = "downsample.period";
    private static final String MAX_DOWNSAMPLE_MEMORY = "downsample.maxDownsampleMemory";
    private static final String AGGCLASS = "downsample.aggclass";

    private static final long DEFAULT_DOWNSAMPLE_MS = 1;
    private static final String DEFAULT_DOWNSAMPLE_AGGREGATOR = Avg.class.getSimpleName().toLowerCase();

    private DownsampleFactory factory;
    private final Map<Set<Tag>, Downsample> value = new HashMap<>();
    private long start;
    private long end;
    private long period;
    private Key last;

    private DownsampleMemoryEstimator memoryEstimator = null;

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
        memoryEstimator = new DownsampleMemoryEstimator(maxDownsampleMemory, start, period);

        String aggClassname = options.get(AGGCLASS);
        Class<?> aggClass;
        try {
            aggClass = this.getClass().getClassLoader().loadClass(aggClassname);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        @SuppressWarnings("unchecked")
        Class<? extends Aggregator> uncheckedAggClass = (Class<? extends Aggregator>) aggClass;
        factory = new DownsampleFactory(start, end, period, uncheckedAggClass);
    }

    @Override
    public boolean hasTop() {

        if (super.hasTop()) {
            while (super.hasTop()) {
                Key topKey = super.getTopKey();
                Value topValue = super.getTopValue();
                try {
                    Metric metric = MetricAdapter.parse(topKey, topValue);
                    long timestamp = metric.getValue().getTimestamp();
                    if (memoryEstimator.shouldReturnBasedOnMemoryUsage(timestamp, value)) {
                        LOG.trace("returning current values - memory usage > " + memoryEstimator.maxDownsampleMemory
                                + " for metric=" + metric.toString());
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
                } catch (Exception e) {
                    LOG.error("Error: {} parsing metric at key: {}", e.getMessage(), topKey.toString());
                }
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

    public static Map<Set<Tag>, Downsample> decodeValue(Value value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value.get());
        ObjectInputStream ois = new ObjectInputStream(bis);
        @SuppressWarnings("unchecked")
        Map<Set<Tag>, Downsample> unchecked = (Map<Set<Tag>, Downsample>) ois.readObject();
        return unchecked;
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

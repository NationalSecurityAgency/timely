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
import timely.model.Tag;
import timely.api.response.TimelyException;
import timely.sample.Aggregator;
import timely.sample.Downsample;
import timely.sample.DownsampleFactory;

public class DownsampleIterator extends WrappingIterator {

    private static final String START = "downsample.start";
    private static final String END = "downsample.end";
    private static final String PERIOD = "downsample.period";
    private static final String AGGCLASS = "downsample.aggclass";

    private DownsampleFactory factory;
    private final Map<Set<Tag>, Downsample> value = new HashMap<>();
    private Key last;

    @SuppressWarnings("unchecked")
    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
        long start = Long.parseLong(options.get(START));
        long end = Long.parseLong(options.get(END));
        long period = Long.parseLong(options.get(PERIOD));
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
        while (super.hasTop()) {
            last = super.getTopKey();
            Metric metric = MetricAdapter.parse(super.getTopKey(), super.getTopValue());
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
                throw new RuntimeException(e);
            }
        }
        return last != null;
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
            return new Value(bos.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void next() throws IOException {
        last = null;
    }

    public static void setDownsampleOptions(IteratorSetting is, long start, long end, long period, String classname) {
        is.addOption(START, "" + start);
        is.addOption(END, "" + end);
        is.addOption(PERIOD, "" + period);
        is.addOption(AGGCLASS, classname);
    }

    @SuppressWarnings("unchecked")
    public static Map<Set<Tag>, Downsample> decodeValue(Value value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value.get());
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (Map<Set<Tag>, Downsample>) ois.readObject();
    }
}

package timely.sample.iterators;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.commons.lang3.StringUtils;
import timely.model.Tag;
import timely.sample.Aggregation;
import timely.sample.Aggregator;
import timely.sample.Downsample;
import timely.sample.Sample;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This iterator will aggregate across series as output from the DownsampleIterator.  The same set of aggregator
 * functions are supported as in the DownsampleIterator.
 */
public class AggregationIterator extends WrappingIterator {

    // To keep the output of the aggregation iterator consistent with that of the downsample iterator, we
    // supply a static set of tags to be returned with the Aggregation (subclass of Downsample)
    private static final String TAGS = "aggregation.tags";
    private static final String AGGCLASS = "aggregation.aggclass";

    private Aggregation aggregation;
    private Set<Tag> tags;
    private Key last;

    @SuppressWarnings("unchecked")
    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        super.init(source, options, env);
        String aggClassname = options.get(AGGCLASS);
        Class<? extends Aggregator> aggClass;
        try {
            aggClass = (Class<? extends Aggregator>) this.getClass().getClassLoader().loadClass(aggClassname);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        tags = deserializeTags(options.get(TAGS));
        try {
            aggregation = new Aggregation(aggClass.newInstance());
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Error creating aggregator class: " + aggClass, e);
        }
    }

    @Override
    public boolean hasTop() {
        // for each set of downsamples (one per subquery)
        while (super.hasTop()) {
            last = super.getTopKey();

            // decode the set of samples (or series)
            Map<Set<Tag>, Downsample> samples = null;
            try {
                samples = DownsampleIterator.decodeValue(super.getTopValue());
            } catch (IOException e) {
                throw new RuntimeException("Unable to decode upstream value as a Downsample", e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Unable to find class for value (expected to be a Downsample)", e);
            }

            // add the downsampled values to the aggregation
            for (Map.Entry<Set<Tag>, Downsample> entry : samples.entrySet()) {
                for (Sample sample : entry.getValue()) {
                    aggregation.add(sample.timestamp, sample.value);
                }
            }

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
        // return a value which is consistent with Map<Set<Tag>, Downsample>
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(bos)) {
            Map<Set<Tag>, Aggregation> aggregationMap = new HashMap<>();
            aggregationMap.put(tags, aggregation);
            out.writeObject(aggregationMap);
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

    public static void setAggregationOptions(IteratorSetting is, Map<String, String> tags, String classname) {
        is.addOption(TAGS, serializeTags(tags));
        is.addOption(AGGCLASS, classname);
    }

    public static String serializeTags(Map<String, String> tags) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (builder.length() > 0) {
                builder.append(',');
            }
            builder.append(escape(entry.getKey())).append('=').append(escape(entry.getValue()));
        }
        return builder.toString();
    }

    private static String escape(String tag) {
        try {
            return URLEncoder.encode(tag, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static String unescape(String tag) {
        try {
            return URLDecoder.decode(tag, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Set<Tag> deserializeTags(String tags) {
        Set<Tag> tagSet = new HashSet<>();
        String[] tagArray = StringUtils.split(tags, ',');
        for (String tagStr : tagArray) {
            String[] tagParts = StringUtils.split(tagStr, '=');
            tagSet.add(new Tag(unescape(tagParts[0]), unescape(tagParts[1])));
        }
        return tagSet;
    }

    @SuppressWarnings("unchecked")
    public static Map<Set<Tag>, Aggregation> decodeValue(Value value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(value.get());
        ObjectInputStream ois = new ObjectInputStream(bis);
        return (Map<Set<Tag>, Aggregation>) ois.readObject();
    }
}

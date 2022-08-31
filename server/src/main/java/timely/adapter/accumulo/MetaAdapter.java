package timely.adapter.accumulo;

import org.apache.accumulo.core.data.Key;

import timely.model.Meta;
import timely.model.Metric;

/**
 * Creates Accumulo data structures from {@link Metric}
 */
public class MetaAdapter {

    public static Key createMetricKey(String metricName, Long timestamp) {
        return new Key(Meta.METRIC_PREFIX + metricName, "", "", timestamp);
    }

    public static Key createTagKey(String metricName, String tagName, Long timestamp) {
        return new Key(Meta.TAG_PREFIX + metricName, tagName, "", timestamp);
    }

    public static Key createValueKey(String metricName, String tagName, String tagValue, Long timestamp) {
        return new Key(Meta.VALUE_PREFIX + metricName, tagName, tagValue, timestamp);
    }

    public static Meta parse(Key key) {
        String row = key.getRow().toString();
        if (row.startsWith(Meta.METRIC_PREFIX)) {
            return Meta.parse(key, null, Meta.METRIC_PREFIX);
        } else if (row.startsWith(Meta.TAG_PREFIX)) {
            return Meta.parse(key, null, Meta.TAG_PREFIX);
        } else if (row.startsWith(Meta.VALUE_PREFIX)) {
            return Meta.parse(key, null, Meta.VALUE_PREFIX);
        }
        throw new IllegalStateException("Invalid key in meta " + key);
    }

    public static String decodeRowKey(Key key) {
        return parse(key).getMetric();
    }
}

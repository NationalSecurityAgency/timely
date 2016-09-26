package timely.adapter.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import timely.api.model.Meta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Interface for Meta model to Accumulo
 *
 */
public class MetaAdapter {

    private static final String METRIC_PREFIX = "m:";
    private static final String TAG_PREFIX = "t:";
    private static final String VALUE_PREFIX = "v:";

    public static List<Key> toKeys(Meta meta) {
        List<Key> keys = new ArrayList<>();
        keys.add(new Key(METRIC_PREFIX + meta.getMetric()));
        keys.add(new Key(TAG_PREFIX + meta.getMetric(), meta.getTagKey()));
        keys.add(new Key(VALUE_PREFIX + meta.getMetric(), meta.getTagKey(), meta.getTagValue()));

        return keys;
    }

    public static Meta parse(Key k, Value v) {
        if (k.getColumnQualifier().getLength() > 0) {
            return new Meta(k.getRow().toString().substring(METRIC_PREFIX.length()), k.getColumnFamily().toString(), k
                    .getColumnQualifier().toString());
        } else if (k.getColumnFamily().getLength() > 0) {
            return new Meta(k.getRow().toString().substring(METRIC_PREFIX.length()), k.getColumnFamily().toString(),
                    null);

        } else {
            return new Meta(k.getRow().toString().substring(METRIC_PREFIX.length()), null, null);
        }
    }

    public static String parseMetricFromMetricRow(Text row) {
        return row.toString().substring(MetaAdapter.METRIC_PREFIX.length());
    }

    private static Text getRawStart(String prefix) {
        return new Text(prefix);
    }

    private static Text getStart(String prefix, String metric) {
        return new Text(prefix + metric);
    }

    private static Text getEnd(String prefix, String metric) {
        return new Text(prefix + metric + "\\x0000");
    }

    public static Text getMetricStart(String metric) {
        return getStart(METRIC_PREFIX, metric);
    }

    public static Text getMetricEnd(String metric) {
        return getEnd(METRIC_PREFIX, metric);
    }

    public static Text getRawMetricStart() {
        return getRawStart(METRIC_PREFIX);
    }

    public static Text getRawMetricEnd() {
        byte last = (byte) 0xff;
        byte[] lastBytes = new byte[100];
        Arrays.fill(lastBytes, last);
        Text end = MetaAdapter.getRawMetricStart();
        new Text(METRIC_PREFIX);
        end.append(lastBytes, 0, lastBytes.length);

        return end;
    }

    public static Text getTagStart(String metric) {
        return getStart(TAG_PREFIX, metric);
    }

    public static Text getTagEnd(String metric) {
        return getEnd(TAG_PREFIX, metric);
    }

    public static Text getRawTagStart() {
        return getRawStart(TAG_PREFIX);
    }

    public static Text getValueStart(String metric) {
        return getStart(VALUE_PREFIX, metric);
    }

    public static Text getValueEnd(String metric) {
        return getEnd(VALUE_PREFIX, metric);
    }

    public static Text getRawValueStart() {
        return getRawStart(VALUE_PREFIX);
    }

    public static Range getMetricRange() {
        return new Range(METRIC_PREFIX, true, TAG_PREFIX, false);
    }
}

package timely.adapter.accumulo;

import com.google.common.base.Joiner;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import timely.model.Metric;
import timely.model.Tag;
import timely.model.parse.TagListParser;
import timely.model.parse.TagParser;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Creates Accumulo data structures from {@link timely.model.Metric}
 */
public class MetricAdapter {

    private static final PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(),
            new LongLexicoder());

    private static final PairLexicoder<Long, String> colQualCoder = new PairLexicoder<>(new LongLexicoder(),
            new StringLexicoder());

    private static final TagParser tagParser = new TagParser();
    private static final TagListParser tagListParser = new TagListParser();

    public static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();
    public static final String VISIBILITY_TAG = "viz";
    public static final Joiner equalsJoiner = Joiner.on("=");

    public static long roundTimestampToLastHour(long timestamp) {
        return timestamp - (timestamp % 3600000);
    }

    public static long roundTimestampToNextHour(long timestamp) {
        return timestamp - (timestamp % 3600000) + 3600000;
    }

    private static List<Tag> escapeDelimiters(List<Tag> tags) {
        List<Tag> newTags = new ArrayList<>();
        for (Tag t : tags) {
            // escape all commas and equals in the tag key and value since they
            // are used as a delimiter
            String k = t.getKey();
            String v = t.getValue();
            k = k.replaceAll("=", "\\=");
            k = k.replaceAll(",", "\\,");
            v = v.replaceAll("=", "\\=");
            v = v.replaceAll(",", "\\,");
            newTags.add(new Tag(k, v));
        }
        return newTags;
    }

    private static Map<String, String> escapeDelimiters(Map<String, String> tags) {
        Map<String, String> newTags = new LinkedHashMap<>();
        for (Map.Entry<String, String> t : tags.entrySet()) {
            // escape all commas and equals in the tag key and value since they
            // are used as a delimiter
            String k = t.getKey();
            String v = t.getValue();
            k = k.replaceAll("=", "\\=");
            k = k.replaceAll(",", "\\,");
            v = v.replaceAll("=", "\\=");
            v = v.replaceAll(",", "\\,");
            newTags.put(k, v);
        }
        return newTags;
    }

    public static Mutation toMutation(Metric metric) {

        final Mutation mutation = new Mutation(encodeRowKey(metric));
        List<Tag> tags = metric.getTags();
        tags = escapeDelimiters(tags);
        Collections.sort(tags);

        for (final Tag entry : tags) {
            if (entry.getKey().equals(VISIBILITY_TAG))
                continue;

            final String cf = entry.join();
            // @formatter:off
            String cq = tags.stream().filter(inner -> !inner.equals(entry))
                    .filter(inner -> !inner.getKey().equals(VISIBILITY_TAG))
                    .map(Tag::join)
                    .collect(Collectors.joining(","));
            // @formatter:on
            byte[] cqBytes = encodeColQual(metric.getValue().getTimestamp(), cq);
            mutation.put(new Text(cf.getBytes(Charset.forName("UTF-8"))), new Text(cqBytes), extractVisibility(tags),
                    metric.getValue().getTimestamp(), extractValue(metric));
        }
        return mutation;
    }

    public static Key toKey(String metric, Map<String, String> tags, long timestamp) {
        byte[] row = encodeRowKey(metric, timestamp);

        tags = escapeDelimiters(tags);
        StringBuilder colQualSb = new StringBuilder();
        String cf = null;
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            if (entry.getKey().equals(VISIBILITY_TAG))
                continue;

            if (cf == null) {
                cf = equalsJoiner.join(entry.getKey(), entry.getValue());
                continue;
            }
            if (colQualSb.length() > 0) {
                colQualSb.append(",");
            }
            colQualSb.append(equalsJoiner.join(entry.getKey(), entry.getValue()));
        }
        byte[] cqBytes = encodeColQual(timestamp, colQualSb.toString());
        ColumnVisibility colVis = extractVisibility(tags);
        return new Key(new Text(row), new Text(cf), new Text(cqBytes), colVis, timestamp);
    }

    private static Value extractValue(Metric metric) {
        return new Value(encodeValue(metric.getValue().getMeasure()));
    }

    public static byte[] encodeValue(Double d) {
        byte[] b = new byte[Double.BYTES];
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.putDouble(d);
        return b;
    }

    public static Double decodeValue(byte[] buf) {
        ByteBuffer bb = ByteBuffer.wrap(buf);
        return bb.getDouble();
    }

    public static ColumnVisibility extractVisibility(List<Tag> tags) {
        // @formatter:off
        Optional<Tag> visTag = tags.stream()
                .filter(t -> t.getKey().equals(VISIBILITY_TAG))
                .findFirst();
        return visTag.isPresent() ? new ColumnVisibility(visTag.get().getValue()) : EMPTY_VISIBILITY;
        // @formatter:on
    }

    public static ColumnVisibility extractVisibility(Map<String, String> tags) {
        if (tags.containsKey(VISIBILITY_TAG)) {
            return new ColumnVisibility(tags.get(VISIBILITY_TAG));
        } else {
            return EMPTY_VISIBILITY;
        }
    }

    public static Metric parse(Key k, Value v, boolean includeVizTag) {
        ComparablePair<String, Long> row = rowCoder.decode(k.getRow().getBytes());
        // @formatter:off
        Metric.Builder builder = Metric.newBuilder()
                .name(row.getFirst())
                .value(k.getTimestamp(), ByteBuffer.wrap(v.get()).getDouble())
                .tag(tagParser.parse(k.getColumnFamily().toString()));
        // @formatter:on
        ComparablePair<Long, String> cq = colQualCoder.decode(k.getColumnQualifier().getBytes());
        tagListParser.parse(cq.getSecond()).forEach(builder::tag);
        if (includeVizTag && k.getColumnVisibility().getLength() > 0) {
            tagListParser.parse("viz=" + k.getColumnVisibility().toString()).forEach(builder::tag);
        }
        return builder.build();
    }

    public static Metric parse(Key k, Value v) {
        return parse(k, v, false);
    }

    public static byte[] encodeRowKey(String metricName, Long timestamp) {
        return rowCoder.encode(new ComparablePair<>(metricName, timestamp));
    }

    public static byte[] encodeRowKey(Metric metric) {
        // round timestamp to hour for scan efficiency and compression
        return encodeRowKey(metric.getName(), roundTimestampToLastHour(metric.getValue().getTimestamp()));
    }

    public static byte[] encodeColQual(Long timestamp, String colQual) {
        return colQualCoder.encode(new ComparablePair<>(timestamp, colQual));
    }

    public static Pair<Long, String> decodeColQual(byte[] colQual) {
        return colQualCoder.decode(colQual);
    }

    public static Pair<String, Long> decodeRowKey(Key k) {
        return rowCoder.decode(k.getRow().getBytes());
    }
}

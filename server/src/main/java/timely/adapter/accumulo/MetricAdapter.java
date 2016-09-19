package timely.adapter.accumulo;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ComparablePair;
import timely.model.Metric;
import timely.model.Tag;
import timely.model.parse.TagListParser;
import timely.model.parse.TagParser;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Creates Accumulo data structures from {@link timely.model.Metric}
 */
public class MetricAdapter {

    private static final PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(),
            new LongLexicoder());

    private static final TagParser tagParser = new TagParser();
    private static final TagListParser tagListParser = new TagListParser();

    public static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();
    public static final String VISIBILITY_TAG = "viz";

    public static Mutation toMutation(Metric metric) {
        final Mutation mutation = new Mutation(encodeRowKey(metric));

        List<Tag> tags = metric.getTags();
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

            mutation.put(cf, cq, extractVisibility(tags), metric.getValue().getTimestamp(), extractValue(metric));
        }
        return mutation;
    }

    private static Value extractValue(Metric metric) {
        byte[] b = new byte[Double.BYTES];
        ByteBuffer bb = ByteBuffer.wrap(b);
        bb.putDouble(metric.getValue().getMeasure());

        return new Value(b);
    }

    private static ColumnVisibility extractVisibility(List<Tag> tags) {
        // @formatter:off
        Optional<Tag> visTag = tags.stream()
                .filter(t -> t.getKey().equals(VISIBILITY_TAG))
                .findFirst();
        return visTag.isPresent() ? new ColumnVisibility(visTag.get().getValue()) : EMPTY_VISIBILITY;
        // @formatter:on
    }

    public static Metric parse(Key k, Value v) {
        ComparablePair<String, Long> row = rowCoder.decode(k.getRow().getBytes());
        // @formatter:off
        Metric.Builder builder = Metric.newBuilder()
                .name(row.getFirst())
                .value(row.getSecond(), ByteBuffer.wrap(v.get()).getDouble())
                .tag(tagParser.parse(k.getColumnFamily().toString()));
        // @formatter:on
        tagListParser.parse(k.getColumnQualifier().toString()).forEach(builder::tag);
        return builder.build();
    }

    public static byte[] encodeRowKey(String metricName, Long timestamp) {
        return rowCoder.encode(new ComparablePair<>(metricName, timestamp));
    }

    public static byte[] encodeRowKey(Metric metric) {
        return encodeRowKey(metric.getName(), metric.getValue().getTimestamp());
    }
}

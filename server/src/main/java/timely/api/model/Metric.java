package timely.api.model;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import timely.api.annotation.Http;
import timely.api.annotation.Tcp;
import timely.api.annotation.WebSocket;
import timely.api.request.HttpPostRequest;
import timely.api.request.TcpRequest;
import timely.auth.VisibilityCache;
import timely.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonIgnore;

@Tcp(operation = "put")
@Http(path = "/api/put")
@WebSocket(operation = "put")
public class Metric implements TcpRequest, HttpPostRequest {

    private static final PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(),
            new LongLexicoder());
    private static final DoubleLexicoder valueCoder = new DoubleLexicoder();

    public static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();
    private static final String VISIBILITY_TAG = "viz=";
    private static final int VISIBILITY_TAG_LENGTH = VISIBILITY_TAG.length();

    private String metric;
    private long timestamp;
    private double value;
    private List<Tag> tags;
    @JsonIgnore
    private ColumnVisibility visibility = EMPTY_VISIBILITY;

    public Metric() {
    }

    public Metric(String metric, long timestamp, double value, List<Tag> tags) {
        super();
        this.metric = metric;
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }

    public void addTag(Tag tag) {
        if (null == this.tags) {
            tags = new ArrayList<Tag>();
        }
        this.tags.add(tag);
    }

    public ColumnVisibility getVisibility() {
        return visibility;
    }

    public void setVisibility(ColumnVisibility visibility) {
        this.visibility = visibility;
    }

    public Mutation toMutation() {
        final byte[] row = rowCoder.encode(new ComparablePair<String, Long>(this.metric, this.timestamp));
        final Value value = new Value(valueCoder.encode(this.value));
        final Mutation m = new Mutation(row);
        Collections.sort(tags);
        for (final Tag entry : tags) {
            final String cf = entry.getKey() + "=" + entry.getValue();
            final StringBuffer otherTags = new StringBuffer();
            String sep = "";
            for (final Tag inner : this.tags) {
                if (inner != entry) {
                    otherTags.append(sep).append(inner.getKey()).append("=").append(inner.getValue());
                    sep = ",";
                }
            }
            m.put(cf, otherTags, visibility, this.timestamp, value);
        }
        return m;
    }

    public static Metric parse(Key k, Value v) {
        ComparablePair<String, Long> row = rowCoder.decode(k.getRow().getBytes());
        Double value = valueCoder.decode(v.get());
        Metric m = new Metric();
        m.setMetric(row.getFirst());
        m.setTimestamp(row.getSecond());
        Tag tag = new Tag(k.getColumnFamily().toString());
        m.addTag(tag);
        String qualifier = k.getColumnQualifier().toString();
        if (!qualifier.isEmpty()) {
            for (String otherTag : qualifier.split(",")) {
                m.addTag(new Tag(otherTag));
            }
        }
        m.setValue(value);
        return m;
    }

    public static byte[] encodeRowKey(String metric, Long timestamp) {
        return rowCoder.encode(new ComparablePair<String, Long>(metric, timestamp));
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("[metric = ").append(metric);
        buf.append(", timestamp = ").append(timestamp);
        tags.forEach(t -> buf.append(", ").append(t.getKey()).append(" = ").append(t.getValue()));
        buf.append(", value = ").append(value).append("]");
        return buf.toString();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hc = new HashCodeBuilder(17, 37);
        hc.append(metric);
        hc.append(timestamp);
        hc.append(value);
        hc.append(tags);
        return hc.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Metric)) {
            return false;
        }
        Metric other = (Metric) obj;
        EqualsBuilder equals = new EqualsBuilder();
        equals.append(this.metric, other.metric);
        equals.append(this.timestamp, other.timestamp);
        equals.append(this.value, other.value);
        equals.append(this.tags, other.tags);
        return equals.isEquals();
    }

    @Override
    public void parse(String line) {
        // put specification
        //
        // put <metricName> <timestamp> <value> <tagK=tagV> <tagK=tagV> ...
        String[] parts = line.split(" ");
        this.setMetric(parts[1]);
        long ts = Long.parseLong(parts[2]);
        if (ts < 9999999999L) {
            ts *= 1000;
        }
        this.setTimestamp(ts);
        this.setValue(Double.valueOf(parts[3]));
        String part;
        for (int i = 4; i < parts.length; i++) {
            part = parts[i];
            if (part.startsWith(VISIBILITY_TAG) && part.length() > VISIBILITY_TAG_LENGTH) {
                this.setVisibility(VisibilityCache.getColumnVisibility(part.substring(VISIBILITY_TAG_LENGTH)));
            } else if (!part.isEmpty()) {
                this.addTag(new Tag(parts[i]));
            }
        }

    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        return JsonUtil.getObjectMapper().readValue(content.getBytes(StandardCharsets.UTF_8), Metric.class);
    }

}

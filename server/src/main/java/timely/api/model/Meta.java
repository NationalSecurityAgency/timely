package timely.api.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class Meta implements Comparable<Meta> {

    public static final String METRIC_PREFIX = "m:";
    public static final String TAG_PREFIX = "t:";
    public static final String VALUE_PREFIX = "v:";

    private String metric;
    private String tagKey;
    private String tagValue;

    public Meta(String metric, String tagKey, String tagValue) {
        super();
        this.metric = metric;
        this.tagKey = tagKey;
        this.tagValue = tagValue;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public String getTagKey() {
        return tagKey;
    }

    public void setTagKey(String tagKey) {
        this.tagKey = tagKey;
    }

    public String getTagValue() {
        return tagValue;
    }

    public void setTagValue(String tagValue) {
        this.tagValue = tagValue;
    }

    public List<Key> toKeys() {
        List<Key> keys = new ArrayList<>();
        if (StringUtils.isNotBlank(metric)) {
            keys.add(new Key(METRIC_PREFIX + metric));
        }
        if (StringUtils.isNotBlank(tagKey)) {
            keys.add(new Key(TAG_PREFIX + metric, tagKey));
        }
        if (StringUtils.isNotBlank(tagKey) && StringUtils.isNotBlank(tagValue)) {
            keys.add(new Key(VALUE_PREFIX + metric, tagKey, tagValue));
        }
        return keys;
    }

    public static String prefix(Key key) {
        String row = key.getRow().toString();
        if (row.startsWith(Meta.METRIC_PREFIX)) {
            return Meta.METRIC_PREFIX;
        } else if (row.startsWith(Meta.TAG_PREFIX)) {
            return Meta.TAG_PREFIX;
        } else if (row.startsWith(Meta.VALUE_PREFIX)) {
            return Meta.VALUE_PREFIX;
        }
        throw new IllegalStateException("Invalid key in meta " + key.toString());
    }

    public static Meta parse(Key key) {
        return parse(key, null);
    }

    public static Meta parse(Key key, Value value) {
        String row = key.getRow().toString();
        if (row.startsWith(Meta.METRIC_PREFIX)) {
            return Meta.parse(key, value, Meta.METRIC_PREFIX);
        } else if (row.startsWith(Meta.TAG_PREFIX)) {
            return Meta.parse(key, value, Meta.TAG_PREFIX);
        } else if (row.startsWith(Meta.VALUE_PREFIX)) {
            return Meta.parse(key, value, Meta.VALUE_PREFIX);
        }
        throw new IllegalStateException("Invalid key in meta " + key.toString());
    }

    public static Meta parse(Key k, Value v, String prefix) {
        if (k.getColumnQualifier().getLength() > 0) {
            return new Meta(k.getRow().toString().substring(prefix.length()), k.getColumnFamily().toString(), k.getColumnQualifier().toString());
        } else if (k.getColumnFamily().getLength() > 0) {
            return new Meta(k.getRow().toString().substring(prefix.length()), k.getColumnFamily().toString(), null);

        } else {
            return new Meta(k.getRow().toString().substring(prefix.length()), null, null);
        }
    }

    @Override
    public int hashCode() {
        HashCodeBuilder builder = new HashCodeBuilder();
        builder.append(metric);
        builder.append(tagKey);
        builder.append(tagValue);
        return builder.toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Meta) {
            Meta obj = (Meta) o;
            EqualsBuilder builder = new EqualsBuilder();
            builder.append(this.metric, obj.metric);
            builder.append(this.tagKey, obj.tagKey);
            builder.append(this.tagValue, obj.tagValue);
            return builder.isEquals();
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "MetaKey [metric=" + metric + ", tagKey=" + tagKey + ", tagValue=" + tagValue + "]";
    }

    @Override
    public int compareTo(Meta o) {
        CompareToBuilder builder = new CompareToBuilder();
        builder.append(this.metric, o.metric);
        builder.append(this.tagKey, o.tagKey);
        builder.append(this.tagValue, o.tagValue);
        return builder.toComparison();
    }
}

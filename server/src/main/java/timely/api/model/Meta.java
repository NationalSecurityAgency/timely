package timely.api.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

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
        keys.add(new Key(METRIC_PREFIX + metric));
        keys.add(new Key(TAG_PREFIX + metric, tagKey));
        keys.add(new Key(VALUE_PREFIX + metric, tagKey, tagValue));
        return keys;
    }

    public static Meta parse(Key k, Value v) {
        return Meta.parse(k, v, METRIC_PREFIX);
    }

    public static Meta parse(Key k, Value v, String prefix) {
        if (k.getColumnQualifier().getLength() > 0) {
            return new Meta(k.getRow().toString().substring(prefix.length()), k.getColumnFamily().toString(),
                    k.getColumnQualifier().toString());
        } else if (k.getColumnFamily().getLength() > 0) {
            return new Meta(k.getRow().toString().substring(prefix.length()), k.getColumnFamily().toString(), null);

        } else {
            return new Meta(k.getRow().toString().substring(prefix.length()), null, null);
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((metric == null) ? 0 : metric.hashCode());
        result = prime * result + ((tagKey == null) ? 0 : tagKey.hashCode());
        result = prime * result + ((tagValue == null) ? 0 : tagValue.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Meta other = (Meta) obj;
        if (metric == null) {
            if (other.metric != null)
                return false;
        } else if (!metric.equals(other.metric))
            return false;
        if (tagKey == null) {
            if (other.tagKey != null)
                return false;
        } else if (!tagKey.equals(other.tagKey))
            return false;
        if (tagValue == null) {
            if (other.tagValue != null)
                return false;
        } else if (!tagValue.equals(other.tagValue))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "MetaKey [metric=" + metric + ", tagKey=" + tagKey + ", tagValue=" + tagValue + "]";
    }

    @Override
    public int compareTo(Meta o) {
        int result = metric.compareTo(o.metric);
        if (result != 0) {
            return result;
        }
        result = tagKey.compareTo(o.tagKey);
        if (result != 0) {
            return result;
        }
        return tagValue.compareTo(o.tagValue);
    }

}

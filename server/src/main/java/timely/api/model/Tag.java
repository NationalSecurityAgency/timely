package timely.api.model;

import java.io.Serializable;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class Tag implements Comparable<Tag>, Serializable {

    private static final long serialVersionUID = 1L;

    private String key;
    private String value;

    public Tag() {
        super();
    }

    public Tag(String tagValue) {
        super();
        String[] tv = tagValue.split("=");
        if (tv.length != 2) {
            throw new IllegalArgumentException("Invalid tag format: " + tagValue);
        }
        this.key = tv[0];
        this.value = tv[1];
    }

    public Tag(String key, String value) {
        super();
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(key);
        hcb.append(value);
        return hcb.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (obj instanceof Tag) {
            Tag other = (Tag) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(this.key, other.key);
            eq.append(this.value, other.value);
            return eq.isEquals();
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("key", key);
        tsb.append("value", value);
        return tsb.toString();
    }

    @Override
    public int compareTo(Tag o) {
        int result = key.compareTo(o.key);
        if (result != 0) {
            return result;
        }
        return value.compareTo(o.value);
    }

}

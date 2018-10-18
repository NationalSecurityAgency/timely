package timely.model;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import timely.model.parse.TagParser;

/**
 * Tag consists of key value pair
 */
@JsonRootName("tag")
public class Tag implements Comparable<Tag>, Serializable {

    private static final long serialVersionUID = 1L;

    private static final TagParser tagParser = new TagParser();

    private String key;
    private String value;

    public Tag() {
    }

    public Tag(Tag other) {
        this.setKey(other.getKey());
        this.setValue(other.getValue());
    }

    public Tag(String tag) {
        this(tagParser.parse(tag));
    }

    public Tag(String key, String value) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        this.setKey(key);
        this.setValue(value);
    }

    @JsonAnyGetter
    public Map<String, String> get() {
        return Stream.of(this).collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    @JsonAnySetter
    public void set(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @JsonIgnore
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    @JsonIgnore
    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Joins key and value into key=value
     * 
     * @return
     */
    public String join() {
        return tagParser.combine(this);
    }

    @Override
    public String toString() {
        return "Tag{" + join() + "}";
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (null == o || getClass() != o.getClass())
            return false;
        Tag other = (Tag) o;
        return new EqualsBuilder().append(this.key, other.key).append(this.value, other.value).isEquals();
    }

    @Override
    public int compareTo(Tag other) {
        return new CompareToBuilder().append(this.key, other.key).append(this.value, other.value).toComparison();
    }
}

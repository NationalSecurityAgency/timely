package timely.model;

import com.fasterxml.jackson.annotation.*;
import timely.model.parse.TagParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A Metric consists of a metric name, tags, and a Value
 *
 * Json has been flattened to reduce overhead. Example:
 * {"name":"m1","timestamp":1,"measure":1.0,"tags":[{"k1":"v1"}]}
 *
 */
@JsonRootName("metric")
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "name" })
public class Metric {

    private String name;
    private List<Tag> tags;
    private Value value;

    public Metric() {
        tags = new ArrayList<>();
        value = new Value();
    }

    public Metric(Metric other) {
        this();
        this.name = other.getName();
        this.tags.addAll(other.getTags());
        this.value = new Value(other.getValue());
    }

    public Metric(String name, long timestamp, double measure) {
        this();
        this.name = name;
        this.value.setTimestamp(timestamp);
        this.value.setMeasure(measure);
    }

    public Metric(String name, long timestamp, double measure, List<Tag> tags) {
        this(name, timestamp, measure);
        this.setTags(tags);
    }

    public Metric(String name, List<Tag> tags) {
        this();
        this.name = name;
        this.setTags(tags);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @JsonGetter("name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @JsonGetter("tags")
    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags.addAll(tags);
    }

    public void addTag(Tag tag) {
        tags.add(new Tag(tag));
    }

    @JsonUnwrapped
    public Value getValue() {
        return value;
    }

    public void setValue(Value value) {
        this.value = value;
    }

    @Override
    public String toString() {
        Collections.sort(tags);
        return "Metric{" + "name='" + name + '\'' + ", tags=" + tags + ", value=" + value + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Metric metric = (Metric) o;

        if (name != null ? !name.equals(metric.name) : metric.name != null)
            return false;

        if (tags != null) {
            if (tags.size() != metric.tags.size()) {
                return false;
            } else if (!tags.containsAll(metric.tags)) {
                return false;
            }
        }

        return value != null ? value.equals(metric.value) : metric.value == null;

    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    /**
     * Simple Builder for Metric. Note: cannot re-use a builder after build is
     * called.
     */
    public static class Builder {

        Metric metric;

        private Builder() {
            metric = new Metric();
        }

        private static TagParser parser = new TagParser();

        public Builder tag(Tag tag) {
            metric.addTag(tag);
            return this;
        }

        public Builder tags(List<Tag> tags) {
            metric.setTags(tags);
            return this;
        }

        public Builder tag(String key, String value) {
            metric.addTag(new Tag(key, value));
            return this;
        }

        public Builder tag(String rawTag) {
            metric.addTag(parser.parse(rawTag));
            return this;
        }

        public Builder name(String name) {
            metric.setName(name);
            return this;
        }

        public Builder value(Value value) {
            metric.setValue(value);
            return this;
        }

        public Builder value(long timestamp, double measure) {
            metric.setValue(new Value(timestamp, measure));
            return this;
        }

        public Metric build() {
            return this.metric;
        }
    }
}

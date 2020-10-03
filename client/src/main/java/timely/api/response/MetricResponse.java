package timely.api.response;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import timely.model.Metric;
import timely.model.Tag;

@JsonPropertyOrder({ "metric", "timestamp", "value", "tags", "subscriptionId", "complete" })
public class MetricResponse {

    private String metric;
    private long timestamp;
    private double value;
    private List<Tag> tags;
    private String subscriptionId;
    private boolean complete = false;

    public String getMetric() {
        return metric;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getValue() {
        return value;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    public void setComplete(boolean closed) {
        this.complete = closed;
    }

    public static MetricResponse fromMetric(Metric metric, String subscriptionId) {
        MetricResponse response = new MetricResponse();
        response.setMetric(metric.getName());
        response.setTimestamp(metric.getValue().getTimestamp());
        response.setTags(metric.getTags());
        response.setValue(metric.getValue().getMeasure());
        response.setSubscriptionId(subscriptionId);
        response.setComplete(false);
        return response;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hash = new HashCodeBuilder();
        hash.append(this.metric);
        hash.append(this.timestamp);
        hash.append(this.value);
        hash.append(this.tags);
        hash.append(this.subscriptionId);
        hash.append(this.complete);
        return hash.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MetricResponse)) {
            return false;
        }
        MetricResponse other = (MetricResponse) obj;
        EqualsBuilder equals = new EqualsBuilder();
        equals.append(this.metric, other.metric);
        equals.append(this.timestamp, other.timestamp);
        equals.append(this.value, other.value);
        equals.append(this.tags, other.tags);
        equals.append(this.subscriptionId, other.subscriptionId);
        equals.append(this.complete, other.complete);
        return equals.isEquals();
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("metric", this.metric);
        tsb.append("timestamp", this.timestamp);
        tsb.append("value", this.value);
        tsb.append("tags", this.tags);
        tsb.append("subscriptionId", this.subscriptionId);
        tsb.append("complete", this.complete);
        return tsb.toString();
    }

}

package timely.api;

import java.util.Map;
import java.util.Optional;

public class AddSubscription extends SubscriptionRequest {

    private String operation = "add";
    private String metric = null;
    private Optional<Map<String, String>> tags = Optional.empty();
    private Optional<Long> startTime = Optional.empty();
    private Optional<Long> delayTime = Optional.empty();

    public String getOperation() {
        return operation;
    }

    public String getMetric() {
        return metric;
    }

    public Optional<Map<String, String>> getTags() {
        return tags;
    }

    public Optional<Long> getStartTime() {
        return startTime;
    }

    public Optional<Long> getDelayTime() {
        return delayTime;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public void setTags(Optional<Map<String, String>> tags) {
        this.tags = tags;
    }

    public void setStartTime(Optional<Long> startTime) {
        this.startTime = startTime;
    }

    public void setDelayTime(Optional<Long> delayTime) {
        this.delayTime = delayTime;
    }

    @Override
    public int hashCode() {
        return (operation + subscriptionId + metric + tags + startTime + delayTime).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof AddSubscription) {
            AddSubscription other = (AddSubscription) obj;
            return (this.subscriptionId.equals(other.subscriptionId) && this.metric.equals(other.metric)
                    && this.tags.equals(other.tags) && this.startTime.equals(other.startTime) && this.delayTime
                        .equals(other.delayTime));
        } else {
            return false;
        }
    }

}

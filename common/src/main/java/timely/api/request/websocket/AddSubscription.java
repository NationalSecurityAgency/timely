package timely.api.request.websocket;

import java.util.Map;
import java.util.Optional;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "add")
public class AddSubscription extends SubscriptionRequest {

    private String metric = null;
    private Optional<Map<String,String>> tags = Optional.empty();
    private Optional<Long> startTime = Optional.empty();
    private Optional<Long> endTime = Optional.empty();
    private Optional<Long> delayTime = Optional.empty();

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public Optional<Map<String,String>> getTags() {
        return tags;
    }

    public void setTags(Map<String,String> tags) {
        this.tags = Optional.ofNullable(tags);
    }

    public Optional<Long> getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = Optional.ofNullable(startTime);
    }

    public Optional<Long> getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = Optional.ofNullable(endTime);
    }

    public Optional<Long> getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = Optional.ofNullable(delayTime);
    }

}

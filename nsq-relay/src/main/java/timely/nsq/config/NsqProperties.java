package timely.nsq.config;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "timely.nsq")
public class NsqProperties {
    @NotNull
    private List<String> lookupdHttpAddress;
    private Integer lookupIntervalSecs = 2;
    private Integer maxLookupFailuresBeforeError = 10;
    private String topic;
    private String channel;
    private Integer maxInFlight = 1000;
    private Integer maxDeliveryAttempts = 5;
    private Integer messageHandlerThreads = 20;
    private Integer messageHandlerQueueSize = 1000;

    public void setLookupdHttpAddress(List<String> lookupdHttpAddress) {
        this.lookupdHttpAddress = lookupdHttpAddress;
    }

    public List<String> getLookupdHttpAddress() {
        return lookupdHttpAddress;
    }

    public void setLookupIntervalSecs(Integer lookupIntervalSecs) {
        this.lookupIntervalSecs = lookupIntervalSecs;
    }

    public Integer getLookupIntervalSecs() {
        return lookupIntervalSecs;
    }

    public void setMaxLookupFailuresBeforeError(Integer maxLookupFailuresBeforeError) {
        this.maxLookupFailuresBeforeError = maxLookupFailuresBeforeError;
    }

    public Integer getMaxLookupFailuresBeforeError() {
        return maxLookupFailuresBeforeError;
    }

    public void setMaxInFlight(Integer maxInFlight) {
        this.maxInFlight = maxInFlight;
    }

    public Integer getMaxInFlight() {
        return maxInFlight;
    }

    public void setMaxDeliveryAttempts(Integer maxDeliveryAttempts) {
        this.maxDeliveryAttempts = maxDeliveryAttempts;
    }

    public Integer getMaxDeliveryAttempts() {
        return maxDeliveryAttempts;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getChannel() {
        return channel;
    }

    public void setMessageHandlerThreads(Integer messageHandlerThreads) {
        this.messageHandlerThreads = messageHandlerThreads;
    }

    public Integer getMessageHandlerThreads() {
        return messageHandlerThreads;
    }

    public void setMessageHandlerQueueSize(Integer messageHandlerQueueSize) {
        this.messageHandlerQueueSize = messageHandlerQueueSize;
    }

    public Integer getMessageHandlerQueueSize() {
        return messageHandlerQueueSize;
    }
}

package timely.nsq.config;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "timely.client")
public class TimelyClientProperties {
    @NotBlank
    private String host;
    @NotNull
    private Integer tcpPort;
    private Long timeToLiveMs;
    private Long maxLatencyMs;
    private Integer writesToBuffer = -1;
    private Integer relayHandlerTheads = 4;

    public void setHost(String host) {
        this.host = host;
    }

    public String getHost() {
        return host;
    }

    public void setTcpPort(Integer tcpPort) {
        this.tcpPort = tcpPort;
    }

    public Integer getTcpPort() {
        return tcpPort;
    }

    public void setTimeToLiveMs(Long timeToLiveMs) {
        this.timeToLiveMs = timeToLiveMs;
    }

    public Long getTimeToLiveMs() {
        return timeToLiveMs;
    }

    public void setMaxLatencyMs(Long maxLatencyMs) {
        this.maxLatencyMs = maxLatencyMs;
    }

    public Long getMaxLatencyMs() {
        return maxLatencyMs;
    }

    public void setWritesToBuffer(Integer writesToBuffer) {
        this.writesToBuffer = writesToBuffer;
    }

    public Integer getWritesToBuffer() {
        return writesToBuffer;
    }

    public void setRelayHandlerTheads(Integer relayHandlerTheads) {
        this.relayHandlerTheads = relayHandlerTheads;
    }

    public Integer getRelayHandlerTheads() {
        return relayHandlerTheads;
    }
}

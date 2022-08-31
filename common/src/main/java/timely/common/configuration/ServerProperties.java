package timely.common.configuration;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.server")
public class ServerProperties {
    @Autowired
    TimelyProperties timelyProperties;
    @NotBlank
    private String ip;
    @NotNull
    private Integer tcpBasePort;
    @NotNull
    private Integer udpBasePort;
    private Integer shutdownQuietPeriod = 5;

    private int getPortOffset() {
        return (timelyProperties.getInstance() - 1) * timelyProperties.getPortIncrement();
    }

    @NotNull
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getTcpPort() {
        return tcpBasePort + getPortOffset();
    }

    public void setTcpBasePort(int tcpPort) {
        this.tcpBasePort = tcpPort;
    }

    public Integer getTcpBasePort() {
        return tcpBasePort;
    }

    public int getUdpPort() {
        return udpBasePort + getPortOffset();
    }

    public void setUdpBasePort(Integer udpBasePort) {
        this.udpBasePort = udpBasePort;
    }

    public Integer getUdpBasePort() {
        return udpBasePort;
    }

    /**
     * Time to wait (in seconds) for connections to finish and to make sure no new connections happen before shutting down Netty event loop groups.
     */
    public int getShutdownQuietPeriod() {
        return this.shutdownQuietPeriod;
    }

    public void setShutdownQuietPeriod(Integer quietPeriod) {
        this.shutdownQuietPeriod = quietPeriod;
    }
}

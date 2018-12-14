package timely.configuration;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;

public class Server {

    @NotBlank
    private String ip;
    @NotNull
    private Integer tcpPort;
    @NotNull
    private Integer udpPort;
    private Integer shutdownQuietPeriod = 5;

    @NotNull
    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
    }

    public int getUdpPort() {
        return udpPort;
    }

    public void setUdpPort(Integer udpPort) {
        this.udpPort = udpPort;
    }

    /**
     * Time to wait (in seconds) for connections to finish and to make sure no new
     * connections happen before shutting down Netty event loop groups.
     *
     * @return
     */
    public int getShutdownQuietPeriod() {
        return this.shutdownQuietPeriod;
    }

    public void setShutdownQuietPeriod(Integer quietPeriod) {
        this.shutdownQuietPeriod = quietPeriod;
    }
}

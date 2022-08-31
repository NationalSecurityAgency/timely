package timely;

import static org.apache.commons.lang3.builder.ToStringStyle.SHORT_PREFIX_STYLE;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class ServerDetails {

    private String host;
    private Integer tcpPort;
    private Integer httpPort;
    private Integer wsPort;
    private Integer udpPort;

    public ServerDetails() {

    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getTcpPort() {
        return tcpPort;
    }

    public void setTcpPort(Integer tcpPort) {
        this.tcpPort = tcpPort;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public void setHttpPort(Integer httpPort) {
        this.httpPort = httpPort;
    }

    public Integer getWsPort() {
        return wsPort;
    }

    public void setWsPort(Integer wsPort) {
        this.wsPort = wsPort;
    }

    public Integer getUdpPort() {
        return udpPort;
    }

    public void setUdpPort(Integer udpPort) {
        this.udpPort = udpPort;
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this, SHORT_PREFIX_STYLE);
        builder.append(host).append(tcpPort).append(httpPort).append(wsPort).append(udpPort);
        return builder.toString();
    }
}

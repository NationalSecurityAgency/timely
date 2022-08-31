package timely.balancer.test;

public class TestServerInfo {

    public String host;
    public String ip;
    public Integer tcpPort;
    public Integer httpPort;
    public Integer wsPort;
    public Integer udpPort;

    public TestServerInfo(String host, String ip, Integer tcpPort, Integer httpPort, Integer wsPort, Integer udpPort) {
        this.host = host;
        this.ip = ip;
        this.tcpPort = tcpPort;
        this.httpPort = httpPort;
        this.wsPort = wsPort;
        this.udpPort = udpPort;
    }

    static public TestServerInfo of(String host, String ip, Integer tcpPort, Integer httpPort, Integer wsPort, Integer udpPort) {
        return new TestServerInfo(host, ip, tcpPort, httpPort, wsPort, udpPort);
    }

    public String getHost() {
        return host;
    }

    public String getIp() {
        return ip;
    }

    public Integer getTcpPort() {
        return tcpPort;
    }

    public Integer getHttpPort() {
        return httpPort;
    }

    public Integer getWsPort() {
        return wsPort;
    }

    public Integer getUdpPort() {
        return udpPort;
    }
}

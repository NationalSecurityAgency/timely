package timely.grafana.auth.configuration;

public class Grafana {

    private String host = "localhost";
    private Integer port = 3000;

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }
}

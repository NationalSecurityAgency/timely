package timely.balancer.configuration;

import javax.validation.constraints.NotBlank;

public class ZooKeeper {

    private String servers;

    @NotBlank
    private String timeout = "120s";

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getTimeout() {
        return timeout;
    }

    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }
}

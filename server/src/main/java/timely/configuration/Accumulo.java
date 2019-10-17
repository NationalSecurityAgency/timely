package timely.configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import org.springframework.boot.context.properties.NestedConfigurationProperty;

public class Accumulo {

    @NotBlank
    private String zookeepers;
    @NotBlank
    private String instanceName;
    @NotBlank
    private String zookeeperTimeout = "120s";
    @NotBlank
    private String username;
    @NotBlank
    private String password;
    @Valid
    @NestedConfigurationProperty
    private Write write = new Write();
    @Valid
    @NestedConfigurationProperty
    private Scan scan = new Scan();

    public String getInstanceName() {
        return instanceName;
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }

    public Write getWrite() {
        return write;
    }

    public Scan getScan() {
        return scan;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getZookeeperTimeout() {
        return zookeeperTimeout;
    }

    public void setZookeeperTimeout(String zookeeperTimeout) {
        this.zookeeperTimeout = zookeeperTimeout;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}

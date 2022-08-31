package timely.common.configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

@ConfigurationProperties(prefix = "timely.accumulo")
public class AccumuloProperties {

    @NotBlank
    private String instanceName;
    @NotBlank
    private String username;
    @NotBlank
    private String password;
    private int accumuloClientPoolSize = 16;
    @Valid
    @NestedConfigurationProperty
    private WriteProperties writeProperties = new WriteProperties();
    @Valid
    @NestedConfigurationProperty
    private ScanProperties scanProperties = new ScanProperties();

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public WriteProperties getWrite() {
        return writeProperties;
    }

    public ScanProperties getScan() {
        return scanProperties;
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

    public void setAccumuloClientPoolSize(int accumuloClientPoolSize) {
        this.accumuloClientPoolSize = accumuloClientPoolSize;
    }

    public int getAccumuloClientPoolSize() {
        return accumuloClientPoolSize;
    }
}

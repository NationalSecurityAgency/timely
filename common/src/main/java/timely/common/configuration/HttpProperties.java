package timely.common.configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.http")
public class HttpProperties {
    @Autowired
    TimelyProperties timelyProperties;
    @NotBlank
    private String ip;
    @NotNull
    private Integer basePort;
    @NotNull
    private String host;
    private String redirectPath = "/secure-me";
    private long strictTransportMaxAge = 604800;
    @Valid
    @NestedConfigurationProperty
    private CorsProperties corsProperties = new CorsProperties();

    private int getPortOffset() {
        return (timelyProperties.getInstance() - 1) * timelyProperties.getPortIncrement();
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return basePort + getPortOffset();
    }

    public void setBasePort(int basePort) {
        this.basePort = basePort;
    }

    public Integer getBasePort() {
        return basePort;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getRedirectPath() {
        return redirectPath;
    }

    public void setRedirectPath(String redirectPath) {
        this.redirectPath = redirectPath;
    }

    public long getStrictTransportMaxAge() {
        return strictTransportMaxAge;
    }

    public void setStrictTransportMaxAge(long strictTransportMaxAge) {
        this.strictTransportMaxAge = strictTransportMaxAge;
    }

    public CorsProperties getCors() {
        return corsProperties;
    }
}

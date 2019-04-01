package timely.grafana.auth.configuration;

import javax.validation.Valid;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;
import timely.balancer.configuration.BalancerHttp;
import timely.balancer.configuration.BalancerSecurity;

@Validated
@Component
@ConfigurationProperties(prefix = "grafana-auth")
public class GrafanaAuthConfiguration {

    @Valid
    @NestedConfigurationProperty
    private BalancerHttp http = new BalancerHttp();

    @Valid
    @NestedConfigurationProperty
    private BalancerSecurity security = new BalancerSecurity();

    @Valid
    @NestedConfigurationProperty
    private Grafana grafana = new Grafana();

    private Integer shutdownQuietPeriod = 5;

    public BalancerSecurity getSecurity() {
        return security;
    }

    public BalancerHttp getHttp() {
        return http;
    }

    public Grafana getGrafana() {
        return grafana;
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

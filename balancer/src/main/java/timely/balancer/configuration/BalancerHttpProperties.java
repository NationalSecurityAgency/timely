package timely.balancer.configuration;

import javax.validation.Valid;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import timely.common.configuration.HttpProperties;

@RefreshScope
@ConfigurationProperties(prefix = "timely.balancer.http")
public class BalancerHttpProperties extends HttpProperties {

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig<CloseableHttpClient> httpClientPool = new GenericKeyedObjectPoolConfig<>();

    public GenericKeyedObjectPoolConfig<CloseableHttpClient> getHttpClientPool() {
        return httpClientPool;
    }
}

package timely.balancer.configuration;

import javax.validation.Valid;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import timely.configuration.Http;

public class BalancerHttp extends Http {

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig<CloseableHttpClient> httpClientPool = new GenericKeyedObjectPoolConfig<>();

    public GenericKeyedObjectPoolConfig<CloseableHttpClient> getHttpClientPool() {
        return httpClientPool;
    }
}

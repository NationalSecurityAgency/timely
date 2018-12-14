package timely.balancer.configuration;

import javax.validation.Valid;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import timely.configuration.Http;

public class BalancerHttp extends Http {

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig httpClientPool = new GenericKeyedObjectPoolConfig();

    public GenericKeyedObjectPoolConfig getHttpClientPool() {
        return httpClientPool;
    }
}

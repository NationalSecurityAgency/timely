package timely.balancer.configuration;

import javax.validation.Valid;

import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import timely.common.configuration.HttpProperties;

@RefreshScope
@ConfigurationProperties(prefix = "timely.balancer.http")
public class BalancerHttpProperties extends HttpProperties {

    private int connectionRequestTimeout = 5000;
    private int connectTimeout = 5000;
    private int socketTimeout = 300000;
    private int requestTimeout = 300000;

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfiguration<CloseableHttpClient> httpClientPool = new GenericKeyedObjectPoolConfiguration<>();

    public GenericKeyedObjectPoolConfiguration<CloseableHttpClient> getHttpClientPool() {
        return httpClientPool;
    }

    public void setConnectionRequestTimeout(final int connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;
    }

    public int getConnectionRequestTimeout() {
        return connectionRequestTimeout;
    }

    public void setConnectTimeout(final int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setSocketTimeout(final int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }
}

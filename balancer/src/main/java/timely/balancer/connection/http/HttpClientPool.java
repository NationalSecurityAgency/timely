package timely.balancer.connection.http;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.http.impl.client.CloseableHttpClient;

import timely.balancer.configuration.BalancerHttpProperties;
import timely.balancer.connection.TimelyBalancedHost;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.SslClientProperties;

public class HttpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost,CloseableHttpClient> {

    public HttpClientPool(SecurityProperties securityProperties, SslClientProperties sslClientProperties, BalancerHttpProperties http, SSLContext sslContext) {
        super(new HttpClientFactory(securityProperties, sslClientProperties, sslContext), http.getHttpClientPool());
    }
}

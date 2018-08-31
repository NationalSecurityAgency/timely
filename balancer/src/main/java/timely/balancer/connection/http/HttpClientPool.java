package timely.balancer.connection.http;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.http.client.HttpClient;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;

import javax.net.ssl.SSLContext;

public class HttpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost, HttpClient> {

    public HttpClientPool(BalancerConfiguration config, SSLContext sslContext) {
        super(new HttpClientFactory(sslContext), config.getHttpClientPool());
    }
}

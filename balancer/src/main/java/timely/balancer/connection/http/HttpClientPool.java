package timely.balancer.connection.http;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.configuration.BalancerHttp;
import timely.balancer.configuration.BalancerSecurity;
import timely.balancer.connection.TimelyBalancedHost;

public class HttpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost, CloseableHttpClient> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientPool.class);

    public HttpClientPool(BalancerSecurity balancerSecurity, BalancerHttp http, SSLContext sslContext) {
        super(new HttpClientFactory(balancerSecurity, sslContext), http.getHttpClientPool());
    }
}

package timely.balancer.connection.http;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.http.impl.client.CloseableHttpClient;

import timely.balancer.configuration.BalancerHttp;
import timely.balancer.configuration.BalancerSecurity;
import timely.balancer.connection.TimelyBalancedHost;

public class HttpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost,CloseableHttpClient> {

    public HttpClientPool(BalancerSecurity balancerSecurity, BalancerHttp http, SSLContext sslContext) {
        super(new HttpClientFactory(balancerSecurity, sslContext), http.getHttpClientPool());
    }
}

package timely.balancer.connection.ws;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.http.client.HttpClient;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;

import javax.net.ssl.SSLContext;

public class WsClientPool extends GenericKeyedObjectPool<TimelyBalancedHost, HttpClient> {

    public WsClientPool(BalancerConfiguration config, SSLContext sslContext) {
        super(new WsClientFactory(sslContext), config.getHttpClientPool());
    }
}

package timely.balancer.connection.ws;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.http.client.HttpClient;
import timely.Configuration;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;

import javax.net.ssl.SSLContext;

public class WsClientPool extends GenericKeyedObjectPool<TimelyBalancedHost, WebSocketSubscriptionClient> {

    public WsClientPool(Configuration config, BalancerConfiguration balancerConfig, SSLContext sslContext) {
        super(new WsClientFactory(config, sslContext, balancerConfig.isLoginRequired()), balancerConfig
                .getWsClientPool());
    }
}

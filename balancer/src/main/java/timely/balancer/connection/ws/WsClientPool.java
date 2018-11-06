package timely.balancer.connection.ws;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import timely.Configuration;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;

public class WsClientPool extends GenericKeyedObjectPool<TimelyBalancedHost, WebSocketSubscriptionClient> {

    public WsClientPool(Configuration config, BalancerConfiguration balancerConfig, SSLContext sslContext) {
        super(new WsClientFactory(config, sslContext, balancerConfig.isLoginRequired()),
                balancerConfig.getWsClientPool());
    }
}

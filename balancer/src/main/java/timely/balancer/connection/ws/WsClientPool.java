package timely.balancer.connection.ws;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;

public class WsClientPool extends GenericKeyedObjectPool<TimelyBalancedHost, WebSocketSubscriptionClient> {

    public WsClientPool(BalancerConfiguration balancerConfig, SSLContext sslContext) {
        super(new WsClientFactory(balancerConfig, sslContext), balancerConfig.getWebsocket().getWsClientPool());
    }
}

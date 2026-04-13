package timely.balancer.connection.ws;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import timely.balancer.configuration.BalancerProperties;
import timely.balancer.configuration.BalancerWebsocketProperties;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;
import timely.common.configuration.SslClientProperties;

public class WsClientPool extends GenericKeyedObjectPool<TimelyBalancedHost,WebSocketSubscriptionClient> {

    public WsClientPool(BalancerProperties balancerProperties, BalancerWebsocketProperties config, SslClientProperties sslClientProperties,
                    SSLContext sslContext) {
        super(new WsClientFactory(balancerProperties, config, sslClientProperties, sslContext), config.getWsClientPool(),
                        config.getWsClientPool().getAbandonedConfig());
    }
}

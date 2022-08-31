package timely.balancer.connection.ws;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import timely.balancer.configuration.BalancerProperties;
import timely.balancer.configuration.BalancerWebsocketProperties;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;
import timely.common.configuration.SslClientProperties;

public class WsClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost,WebSocketSubscriptionClient> {

    private final SSLContext sslContext;
    private BalancerProperties balancerProperties;
    private BalancerWebsocketProperties balancerWebsocketProperties;
    private SslClientProperties sslClientProperties;

    public WsClientFactory(BalancerProperties balancerProperties, BalancerWebsocketProperties balancerWebsocketProperties,
                    SslClientProperties sslClientProperties, SSLContext sslContext) {
        this.balancerProperties = balancerProperties;
        this.balancerWebsocketProperties = balancerWebsocketProperties;
        this.sslClientProperties = sslClientProperties;
        this.sslContext = sslContext;
    }

    @Override
    public PooledObject<WebSocketSubscriptionClient> makeObject(TimelyBalancedHost k) throws Exception {

        int bufferSize = balancerWebsocketProperties.getIncomingBufferSize();
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(sslContext, k.getHost(), k.getHttpPort(), k.getWsPort(),
                        sslClientProperties.isUseClientCert(), balancerProperties.isLoginRequired(), sslClientProperties.isHostVerificationEnabled(),
                        bufferSize);
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(TimelyBalancedHost k, PooledObject<WebSocketSubscriptionClient> o) throws Exception {

    }

    @Override
    public boolean validateObject(TimelyBalancedHost k, PooledObject<WebSocketSubscriptionClient> o) {
        return true;
    }

    @Override
    public void activateObject(TimelyBalancedHost k, PooledObject<WebSocketSubscriptionClient> o) throws Exception {

    }

    @Override
    public void passivateObject(TimelyBalancedHost k, PooledObject<WebSocketSubscriptionClient> o) throws Exception {

    }
}

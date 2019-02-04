package timely.balancer.connection.ws;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;

public class WsClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost, WebSocketSubscriptionClient> {

    private final SSLContext sslContext;
    private final boolean doLogin;
    private final boolean twoWaySsl;
    private BalancerConfiguration balancerConfig;

    public WsClientFactory(BalancerConfiguration balancerConfig, SSLContext sslContext) {
        this.sslContext = sslContext;
        this.doLogin = balancerConfig.isLoginRequired();
        this.balancerConfig = balancerConfig;
        this.twoWaySsl = balancerConfig.getSecurity().getClientSsl().isTwoWaySsl();
    }

    @Override
    public PooledObject<WebSocketSubscriptionClient> makeObject(TimelyBalancedHost k) throws Exception {

        int bufferSize = balancerConfig.getWebsocket().getSubscriptionBatchSize() * 500;
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(sslContext, k.getHost(), k.getHttpPort(),
                k.getWsPort(), twoWaySsl, doLogin, "", "", false, bufferSize);
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

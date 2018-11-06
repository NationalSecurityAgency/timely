package timely.balancer.connection.ws;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import timely.Configuration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;

public class WsClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost, WebSocketSubscriptionClient> {

    private final SSLContext sslContext;
    private final boolean doLogin;
    private Configuration config;

    public WsClientFactory(Configuration config, SSLContext sslContext, boolean doLogin) {
        this.sslContext = sslContext;
        this.doLogin = doLogin;
        this.config = config;
    }

    @Override
    public PooledObject<WebSocketSubscriptionClient> makeObject(TimelyBalancedHost k) throws Exception {

        int bufferSize = config.getWebsocket().getSubscriptionBatchSize() * 500;
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(sslContext, k.getHost(), k.getHttpPort(),
                k.getWsPort(), doLogin, "", "", false, bufferSize);
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(TimelyBalancedHost k, PooledObject<WebSocketSubscriptionClient> o) throws Exception {

    }

    @Override
    public boolean validateObject(TimelyBalancedHost k, PooledObject<WebSocketSubscriptionClient> o) {
        if (k.isUp()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void activateObject(TimelyBalancedHost k, PooledObject<WebSocketSubscriptionClient> o) throws Exception {

        // try {
        // o.getObject().open();
        // } catch (IOException e) {
        // throw new IOException("Unable to connect to " + k.getHost() + ":" +
        // k.getHttpPort());
        // }
    }

    @Override
    public void passivateObject(TimelyBalancedHost k, PooledObject<WebSocketSubscriptionClient> o) throws Exception {
        // try {
        // o.getObject().flush();
        // } catch (Exception e) {
        // throw new IOException("Error flushing connection to " + k.getHost() +
        // ":" + k.getHttpPort());
        // }
        // try {
        // o.getObject().flush();
        // o.getObject().close();
        // } catch (IOException e) {
        // throw new IOException("Error closing connection to " + k.getHost() +
        // ":" + k.getHttpPort());
        // }
    }
}

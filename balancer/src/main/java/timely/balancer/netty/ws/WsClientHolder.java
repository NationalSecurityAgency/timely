package timely.balancer.netty.ws;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.ws.WsClientPool;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;

public class WsClientHolder {

    private static final Logger log = LoggerFactory.getLogger(WsClientHolder.class);
    private WebSocketSubscriptionClient client;
    private TimelyBalancedHost host;

    public WsClientHolder(TimelyBalancedHost host, WebSocketSubscriptionClient client) {
        this.host = host;
        this.client = client;
    }

    synchronized public WebSocketSubscriptionClient getClient() {
        return client;
    }

    synchronized public TimelyBalancedHost getHost() {
        return host;
    }

    synchronized public void close(WsClientPool wsClientPool) {
        if (client != null && host != null) {
            try {
                client.close();
            } catch (IOException e) {
                log.error("Error closing web socket client", e);
            }
            wsClientPool.returnObject(host, client);
            client = null;
            host = null;
        }
    }
}

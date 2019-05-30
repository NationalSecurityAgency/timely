package timely.balancer.configuration;

import javax.validation.Valid;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;
import timely.configuration.Websocket;

public class BalancerWebsocket extends Websocket {

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig<WebSocketSubscriptionClient> wsClientPool = new GenericKeyedObjectPoolConfig<>();

    public GenericKeyedObjectPoolConfig<WebSocketSubscriptionClient> getWsClientPool() {
        return wsClientPool;
    }
}

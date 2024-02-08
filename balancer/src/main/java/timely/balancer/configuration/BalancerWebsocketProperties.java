package timely.balancer.configuration;

import javax.validation.Valid;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import timely.client.websocket.subscription.WebSocketSubscriptionClient;
import timely.common.configuration.WebsocketProperties;

@RefreshScope
@ConfigurationProperties(prefix = "timely.balancer.websocket")
public class BalancerWebsocketProperties extends WebsocketProperties {

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig<WebSocketSubscriptionClient> wsClientPool = new GenericKeyedObjectPoolConfig<>();

    public GenericKeyedObjectPoolConfig<WebSocketSubscriptionClient> getWsClientPool() {
        return wsClientPool;
    }

    private int incomingBufferSize = 50000000;

    public int getIncomingBufferSize() {
        return incomingBufferSize;
    }

    public void setIncomingBufferSize(int incomingBufferSize) {
        this.incomingBufferSize = incomingBufferSize;
    }
}

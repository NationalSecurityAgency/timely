package timely.server.netty.websocket.subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.websocket.CreateSubscription;
import timely.common.configuration.WebsocketProperties;
import timely.netty.websocket.subscription.SubscriptionConstants;
import timely.server.store.DataStore;
import timely.server.store.cache.DataStoreCache;
import timely.server.subscription.SubscriptionImpl;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSCreateSubscriptionRequestHandler extends SimpleChannelInboundHandler<CreateSubscription> {

    private static final Logger log = LoggerFactory.getLogger(WSCreateSubscriptionRequestHandler.class);
    private final DataStore store;
    private final DataStoreCache cache;
    private final WebsocketProperties websocketProperties;

    public WSCreateSubscriptionRequestHandler(DataStore store, DataStoreCache cache, WebsocketProperties websocketProperties) {
        this.store = store;
        this.cache = cache;
        this.websocketProperties = websocketProperties;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CreateSubscription create) throws Exception {
        final String subscriptionId = create.getSubscriptionId();
        SubscriptionRegistry.get().put(subscriptionId,
                        new SubscriptionImpl(subscriptionId, create.getSessionId(), store, cache, ctx, this.websocketProperties));

        // Store the session id as an attribute on the context.
        ctx.channel().attr(SubscriptionConstants.SUBSCRIPTION_ID_ATTR).set(subscriptionId);
        log.info("[{}] Created subscription on channel {}", subscriptionId, ctx);

        ctx.channel().closeFuture().addListener((ChannelFutureListener) future -> {
            Subscription s = SubscriptionRegistry.get().remove(subscriptionId);
            if (null != s) {
                log.info("[{}] Channel closed, closing subscriptions", subscriptionId);
                s.close();
            }
        });
    }
}

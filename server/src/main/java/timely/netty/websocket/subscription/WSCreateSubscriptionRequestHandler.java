package timely.netty.websocket.subscription;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.api.request.subscription.CreateSubscription;
import timely.store.DataStore;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSCreateSubscriptionRequestHandler extends SimpleChannelInboundHandler<CreateSubscription> {

    private static final Logger LOG = LoggerFactory.getLogger(WSCreateSubscriptionRequestHandler.class);
    private final DataStore store;
    private final Configuration conf;

    public WSCreateSubscriptionRequestHandler(DataStore store, Configuration conf) {
        this.store = store;
        this.conf = conf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CreateSubscription create) throws Exception {
        final String subscriptionId = create.getSubscriptionId();
        SubscriptionRegistry.get().put(subscriptionId,
                new Subscription(subscriptionId, create.getSessionId(), store, ctx, this.conf));

        // Store the session id as an attribute on the context.
        ctx.channel().attr(SubscriptionRegistry.SUBSCRIPTION_ID_ATTR).set(subscriptionId);
        LOG.info("Created subscription {} on channel {}", subscriptionId, ctx);

        ctx.channel().closeFuture().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Subscription s = SubscriptionRegistry.get().remove(subscriptionId);
                if (null != s) {
                    LOG.info("Channel closed, closing subscriptions for subscriptionId: " + subscriptionId);
                    s.close();
                }
            }
        });
    }

}

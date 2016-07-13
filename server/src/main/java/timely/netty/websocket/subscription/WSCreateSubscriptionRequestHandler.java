package timely.netty.websocket.subscription;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.Configuration;
import timely.api.request.subscription.CreateSubscription;
import timely.netty.http.TimelyHttpHandler;
import timely.store.DataStore;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSCreateSubscriptionRequestHandler extends SimpleChannelInboundHandler<CreateSubscription> implements
        TimelyHttpHandler {

    private final DataStore store;
    private final Configuration conf;

    public WSCreateSubscriptionRequestHandler(DataStore store, Configuration conf) {
        this.store = store;
        this.conf = conf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CreateSubscription create) throws Exception {
        SubscriptionRegistry.get().put(create.getSessionId(),
                new Subscription(create.getSessionId(), store, ctx, this.conf));

        // Store the session id as an attribute on the context.
        ctx.attr(SubscriptionRegistry.SESSION_ID_ATTR).set(create.getSessionId());

        ctx.channel().closeFuture().addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                Subscription s = SubscriptionRegistry.get().remove(create.getSessionId());
                if (null != s) {
                    LOG.info("Channel closed, closing subscriptions for sessionId: " + create.getSessionId());
                    s.close();
                }
            }
        });
    }

}

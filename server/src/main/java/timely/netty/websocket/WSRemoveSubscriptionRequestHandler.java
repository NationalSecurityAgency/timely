package timely.netty.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.RemoveSubscription;
import timely.netty.http.TimelyHttpHandler;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSRemoveSubscriptionRequestHandler extends SimpleChannelInboundHandler<RemoveSubscription> implements
        TimelyHttpHandler {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemoveSubscription remove) throws Exception {
        Subscription s = SubscriptionRegistry.get().get(remove.getSessionId());
        if (null != s) {
            s.removeMetric(remove.getMetric());
        }
    }

}

package timely.netty.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.CloseSubscription;
import timely.netty.http.TimelyHttpHandler;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSCloseSubscriptionRequestHandler extends SimpleChannelInboundHandler<CloseSubscription> implements
        TimelyHttpHandler {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CloseSubscription close) throws Exception {
        Subscription s = SubscriptionRegistry.get().remove(close.getSessionId());
        if (null != s) {
            s.close();
        }
    }

}

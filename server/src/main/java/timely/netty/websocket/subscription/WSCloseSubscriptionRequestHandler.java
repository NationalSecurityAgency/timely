package timely.netty.websocket.subscription;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.subscription.CloseSubscription;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSCloseSubscriptionRequestHandler extends SimpleChannelInboundHandler<CloseSubscription> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CloseSubscription close) throws Exception {
        Subscription s = SubscriptionRegistry.get().remove(close.getSessionId());
        if (null != s) {
            s.close();
        }
    }

}

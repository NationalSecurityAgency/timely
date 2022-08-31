package timely.server.netty.websocket.subscription;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import timely.api.CloseSubscription;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSCloseSubscriptionRequestHandler extends SimpleChannelInboundHandler<CloseSubscription> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CloseSubscription close) throws Exception {
        Subscription s = SubscriptionRegistry.get().remove(close.getSubscriptionId());
        if (null != s) {
            s.close();
        }
        ctx.writeAndFlush(new CloseWebSocketFrame(1000, "Client requested close."));
    }

}

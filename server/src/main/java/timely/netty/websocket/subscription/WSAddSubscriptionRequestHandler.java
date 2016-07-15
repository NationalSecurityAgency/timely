package timely.netty.websocket.subscription;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.request.subscription.AddSubscription;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSAddSubscriptionRequestHandler extends SimpleChannelInboundHandler<AddSubscription> {

    private static final Logger LOG = LoggerFactory.getLogger(WSAddSubscriptionRequestHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AddSubscription add) throws Exception {
        Subscription s = SubscriptionRegistry.get().get(add.getSessionId());
        if (null != s) {
            String metric = add.getMetric();
            if (null == metric) {
                LOG.error("Metric name cannot be null in add subscription");
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, "Metric name cannot be null in add subscription"));
            }
            Map<String, String> tags = null;
            Long startTime = 0L;
            Long delayTime = 5000L;
            if (add.getTags().isPresent()) {
                tags = add.getTags().get();
            }
            if (add.getStartTime().isPresent()) {
                startTime = add.getStartTime().get();
            }
            if (add.getDelayTime().isPresent()) {
                delayTime = add.getDelayTime().get();
            }
            s.addMetric(metric, tags, startTime, delayTime);
        } else {
            LOG.error("Unknown session id, create subscription first");
            ctx.writeAndFlush(new CloseWebSocketFrame(1003, "Unknown session id, create subscription first"));
        }
    }

}

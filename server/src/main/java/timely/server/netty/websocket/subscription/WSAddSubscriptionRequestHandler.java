package timely.server.netty.websocket.subscription;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import timely.api.request.websocket.AddSubscription;
import timely.subscription.Subscription;
import timely.subscription.SubscriptionRegistry;

public class WSAddSubscriptionRequestHandler extends SimpleChannelInboundHandler<AddSubscription> {

    private static final Logger log = LoggerFactory.getLogger(WSAddSubscriptionRequestHandler.class);

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AddSubscription add) throws Exception {
        Subscription s = SubscriptionRegistry.get().get(add.getSubscriptionId());
        if (null != s) {
            String metric = add.getMetric();
            if (null == metric) {
                log.error("Metric name cannot be null in add subscription");
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, "Metric name cannot be null in add subscription"));
            }
            Map<String,String> tags = null;
            Long startTime = 0L;
            Long endTime = 0L;
            Long delayTime = 5000L;
            if (add.getTags().isPresent()) {
                tags = add.getTags().get();
            }
            if (add.getStartTime().isPresent()) {
                startTime = add.getStartTime().get();
            }
            if (add.getEndTime().isPresent()) {
                endTime = add.getEndTime().get();
            }
            if (add.getDelayTime().isPresent()) {
                delayTime = add.getDelayTime().get();
            }
            s.addMetric(add, metric, tags, startTime, endTime, delayTime);
        } else {
            log.error("Unknown subscription id, create subscription first");
            ctx.writeAndFlush(new CloseWebSocketFrame(1003, "Unknown subscription id, create subscription first"));
        }
    }

}

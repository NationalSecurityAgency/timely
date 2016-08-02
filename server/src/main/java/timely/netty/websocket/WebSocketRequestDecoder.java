package timely.netty.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.WebSocketRequest;
import timely.api.response.TimelyException;
import timely.auth.AuthCache;
import timely.subscription.SubscriptionRegistry;
import timely.util.JsonUtil;

public class WebSocketRequestDecoder extends MessageToMessageDecoder<WebSocketFrame> {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketRequestDecoder.class);

    private final Configuration conf;

    public WebSocketRequestDecoder(Configuration conf) {
        this.conf = conf;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame msg, List<Object> out) throws Exception {

        if (msg instanceof TextWebSocketFrame) {
            TextWebSocketFrame frame = (TextWebSocketFrame) msg;
            String content = frame.text();
            WebSocketRequest request = JsonUtil.getObjectMapper().readValue(content, WebSocketRequest.class);
            LOG.trace("Received WS request {}", content);

            String sessionId = ctx.channel().attr(SubscriptionRegistry.SESSION_ID_ATTR).get();
            if (request instanceof AuthenticatedRequest && !StringUtils.isEmpty(sessionId)) {
                LOG.info("Found session id in WebSocket channel, setting sessionId {} on request", sessionId);
                AuthenticatedRequest ar = (AuthenticatedRequest) request;
                ar.setSessionId(sessionId);
            }

            try {
                request.validate();
            } catch (IllegalArgumentException e) {
                LOG.error("Error validating web socket request: " + e.getMessage());
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
                return;
            }
            try {
                AuthCache.enforceAccess(conf, request);
            } catch (TimelyException e) {
                out.clear();
                LOG.error("Error during access enforcment: " + e.getMessage());
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
                return;
            }
            out.add(request);

        } else {
            LOG.error("Unhandled web socket frame type");
            ctx.writeAndFlush(new CloseWebSocketFrame(1003,
                    "Unhandled web socket frame type, only TextWebSocketFrame is supported"));
        }
    }

    public static void close() {
        LOG.info("Closing subscriptions");
        SubscriptionRegistry.get().forEach((k, v) -> v.close());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        LOG.error("Error caught", cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idle = (IdleStateEvent) evt;
            if (idle.state() == IdleState.READER_IDLE) {
                // We have not read any data from client in a while, let's close
                // the subscriptions for this context.
                String subscriptionId = ctx.channel().attr(SubscriptionRegistry.SUBSCRIPTION_ID_ATTR).get();
                if (!StringUtils.isEmpty(subscriptionId)) {
                    if (SubscriptionRegistry.get().containsKey(subscriptionId)) {
                        LOG.info("Closing subscription with subscription id {} due to idle event", subscriptionId);
                        SubscriptionRegistry.get().get(subscriptionId).close();
                    }
                } else {
                    LOG.warn("Channel idle, but no subscription id found on context. Unable to close subscriptions");
                }
            }
        } else {
            LOG.warn("Received unhandled user event {}", evt);
        }
    }

}

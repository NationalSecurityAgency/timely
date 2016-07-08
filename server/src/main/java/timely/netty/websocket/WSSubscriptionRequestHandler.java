package timely.netty.websocket;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.api.request.AddSubscription;
import timely.api.request.CloseSubscription;
import timely.api.request.CreateSubscription;
import timely.api.request.RemoveSubscription;
import timely.api.request.WebSocketRequest;
import timely.auth.AuthCache;
import timely.store.DataStore;
import timely.subscription.SubscriptionRegistry;
import timely.util.JsonUtil;

public class WSSubscriptionRequestHandler extends MessageToMessageDecoder<WebSocketFrame> {

    private static final Logger LOG = LoggerFactory.getLogger(WSSubscriptionRequestHandler.class);

    private final Configuration conf;
    private final DataStore store;

    public WSSubscriptionRequestHandler(Configuration conf, DataStore store) {
        this.conf = conf;
        this.store = store;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame msg, List<Object> out) throws Exception {

        if (msg instanceof TextWebSocketFrame) {
            TextWebSocketFrame frame = (TextWebSocketFrame) msg;
            String content = frame.text();
            WebSocketRequest request = JsonUtil.getObjectMapper().readValue(content, WebSocketRequest.class);
            LOG.trace("Received WS request {}", content);
            AuthCache.enforceAccess(conf, request);

            final String sessionId = request.getSessionId();
            if (StringUtils.isEmpty(sessionId)) {
                LOG.error("Session ID must be supplied");
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, "Session ID must be supplied"));
                return;
            }
            out.add(request);

            // if (request instanceof CreateSubscription) {
            // SubscriptionRegistry.get().put(sessionId, new
            // Subscription(sessionId, store, ctx, this.conf));
            //
            // // Store the session id as an attribute on the context.
            // ctx.attr(SubscriptionRegistry.SESSION_ID_ATTR).set(sessionId);
            //
            // ctx.channel().closeFuture().addListener(new
            // ChannelFutureListener() {
            //
            // @Override
            // public void operationComplete(ChannelFuture future) throws
            // Exception {
            // Subscription s = SubscriptionRegistry.get().remove(sessionId);
            // if (null != s) {
            // LOG.info("Channel closed, closing subscriptions for sessionId: "
            // + sessionId);
            // s.close();
            // }
            // }
            // });
            // } else if (request instanceof AddSubscription) {
            // AddSubscription add = (AddSubscription) request;
            // Subscription s = SubscriptionRegistry.get().get(sessionId);
            // if (null != s) {
            // String metric = add.getMetric();
            // if (null == metric) {
            // LOG.error("Metric name cannot be null in add subscription");
            // ctx.writeAndFlush(new CloseWebSocketFrame(1008,
            // "Metric name cannot be null in add subscription"));
            // }
            // Map<String, String> tags = null;
            // Long startTime = 0L;
            // Long delayTime = 5000L;
            // if (add.getTags().isPresent()) {
            // tags = add.getTags().get();
            // }
            // if (add.getStartTime().isPresent()) {
            // startTime = add.getStartTime().get();
            // }
            // if (add.getDelayTime().isPresent()) {
            // delayTime = add.getDelayTime().get();
            // }
            // s.addMetric(metric, tags, startTime, delayTime);
            // } else {
            // LOG.error("Unknown session id, create subscription first");
            // ctx.writeAndFlush(new CloseWebSocketFrame(1003,
            // "Unknown session id, create subscription first"));
            // }
            // } else if (request instanceof RemoveSubscription) {
            // RemoveSubscription remove = (RemoveSubscription) request;
            // Subscription s = SubscriptionRegistry.get().get(sessionId);
            // if (null != s) {
            // s.removeMetric(remove.getMetric());
            // }
            // } else if (request instanceof CloseSubscription) {
            // Subscription s = SubscriptionRegistry.get().remove(sessionId);
            // if (null != s) {
            // s.close();
            // }
            // } else {
            // LOG.error("Unhandled web socket message type");
            // ctx.writeAndFlush(new CloseWebSocketFrame(1003,
            // "Unhandled web socket message type"));
            // }
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
                // the subscriptions
                // for this context.
                String sessionId = ctx.attr(SubscriptionRegistry.SESSION_ID_ATTR).get();
                if (!StringUtils.isEmpty(sessionId)) {
                    if (SubscriptionRegistry.get().containsKey(sessionId)) {
                        LOG.info("Closing subscription with session id {} due to idle event", sessionId);
                        SubscriptionRegistry.get().get(sessionId).close();
                    }
                } else {
                    LOG.warn("Channel idle, but no session id found on context. Unable to close subscriptions");
                }
            }
        } else {
            LOG.warn("Received unhandled user event {}", evt);
        }
    }

}

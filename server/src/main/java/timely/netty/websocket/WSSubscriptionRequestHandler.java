package timely.netty.websocket;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.api.websocket.AddSubscription;
import timely.api.websocket.CloseSubscription;
import timely.api.websocket.CreateSubscription;
import timely.api.websocket.RemoveSubscription;
import timely.api.websocket.WSRequest;
import timely.auth.AuthCache;
import timely.store.DataStore;
import timely.subscription.Subscription;
import timely.util.JsonUtil;

public class WSSubscriptionRequestHandler extends SimpleChannelInboundHandler<WebSocketFrame> {

    private static final Logger LOG = LoggerFactory.getLogger(WSSubscriptionRequestHandler.class);
    private static final Map<String, Subscription> SUBSCRIPTIONS = new ConcurrentHashMap<>();
    private static final AttributeKey<String> SESSION_ID_ATTR = AttributeKey.newInstance("sessionId");

    private final Configuration conf;
    private final DataStore store;

    public WSSubscriptionRequestHandler(Configuration conf, DataStore store) {
        this.conf = conf;
        this.store = store;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, WebSocketFrame msg) throws Exception {

        if (msg instanceof TextWebSocketFrame) {
            TextWebSocketFrame frame = (TextWebSocketFrame) msg;
            String content = frame.text();
            WSRequest request = JsonUtil.getObjectMapper().readValue(content, WSRequest.class);
            LOG.trace("Received WS request {}", content);
            AuthCache.enforceAccess(conf, request);

            final String sessionId = request.getSessionId();
            if (StringUtils.isEmpty(sessionId)) {
                LOG.error("Session ID must be supplied");
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, "Session ID must be supplied"));
                return;
            }

            if (request instanceof CreateSubscription) {
                SUBSCRIPTIONS.put(sessionId, new Subscription(sessionId, store, ctx, this.conf));

                // Store the session id as an attribute on the context.
                ctx.attr(SESSION_ID_ATTR).set(sessionId);

                ctx.channel().closeFuture().addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        Subscription s = SUBSCRIPTIONS.remove(sessionId);
                        if (null != s) {
                            LOG.info("Channel closed, closing subscriptions for sessionId: " + sessionId);
                            s.close();
                        }
                    }
                });
            } else if (request instanceof AddSubscription) {
                AddSubscription add = (AddSubscription) request;
                Subscription s = SUBSCRIPTIONS.get(sessionId);
                if (null != s) {
                    String metric = add.getMetric();
                    if (null == metric) {
                        LOG.error("Metric name cannot be null in add subscription");
                        ctx.writeAndFlush(new CloseWebSocketFrame(1008,
                                "Metric name cannot be null in add subscription"));
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
            } else if (request instanceof RemoveSubscription) {
                RemoveSubscription remove = (RemoveSubscription) request;
                Subscription s = SUBSCRIPTIONS.get(sessionId);
                if (null != s) {
                    s.removeMetric(remove.getMetric());
                }
            } else if (request instanceof CloseSubscription) {
                Subscription s = SUBSCRIPTIONS.remove(sessionId);
                if (null != s) {
                    s.close();
                }
            } else {
                LOG.error("Unhandled web socket message type");
                ctx.writeAndFlush(new CloseWebSocketFrame(1003, "Unhandled web socket message type"));
            }
        } else {
            LOG.error("Unhandled web socket frame type");
            ctx.writeAndFlush(new CloseWebSocketFrame(1003,
                    "Unhandled web socket frame type, only TextWebSocketFrame is supported"));
        }
    }

    public static void close() {
        LOG.info("Closing subscriptions");
        SUBSCRIPTIONS.forEach((k, v) -> v.close());
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
                String sessionId = ctx.attr(SESSION_ID_ATTR).get();
                if (!StringUtils.isEmpty(sessionId)) {
                    if (SUBSCRIPTIONS.containsKey(sessionId)) {
                        LOG.info("Closing subscription with session id {} due to idle event", sessionId);
                        SUBSCRIPTIONS.get(sessionId).close();
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

package timely.balancer.netty.ws;

import java.net.ConnectException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.Multimap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.subscription.AddSubscription;
import timely.api.request.subscription.CloseSubscription;
import timely.api.request.subscription.CreateSubscription;
import timely.api.request.subscription.RemoveSubscription;
import timely.api.request.subscription.SubscriptionRequest;
import timely.api.response.TimelyException;
import timely.auth.util.ProxiedEntityUtils;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.ws.WsClientPool;
import timely.balancer.resolver.MetricResolver;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;
import timely.netty.http.TimelyHttpHandler;

public class WsRelayHandler extends SimpleChannelInboundHandler<SubscriptionRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(WsRelayHandler.class);

    private final BalancerConfiguration balancerConfig;
    private final WsClientPool wsClientPool;
    private MetricResolver metricResolver;
    static private ObjectMapper mapper;
    Map<String, Map<String, WsClientHolder>> wsClients = new ConcurrentHashMap<>();
    Map<String, ChannelHandlerContext> wsSubscriptions = new ConcurrentHashMap<>();

    static {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        mapper.configure(SerializationFeature.WRAP_EXCEPTIONS, true);
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, true);
    }

    public WsRelayHandler(BalancerConfiguration balancerConfig, MetricResolver metricResolver,
            WsClientPool wsClientPool) {
        this.balancerConfig = balancerConfig;
        this.metricResolver = metricResolver;
        this.wsClientPool = wsClientPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SubscriptionRequest msg) throws Exception {
        TimelyBalancedHost k;
        ChannelHandlerContext origContext = null;
        String subscriptionId;
        try {
            String metric = null;

            Multimap<String, String> headers = msg.getRequestHeaders();
            ProxiedEntityUtils.addProxyHeaders(headers, msg.getToken().getClientCert());

            if (msg instanceof CreateSubscription) {
                CreateSubscription create = (CreateSubscription) msg;
                final String currentSubscriptionId = create.getSubscriptionId();
                subscriptionId = currentSubscriptionId;
                wsSubscriptions.put(subscriptionId, ctx);
                // WebSocketSubscriptionClient sends the createSubscription when
                // we call open
                // We don't know where to send it to yet anyway because that
                // depends on the metric.
                ctx.channel().closeFuture().addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        Map<String, WsClientHolder> metricToClientMap = wsClients.get(currentSubscriptionId);

                        if (metricToClientMap != null) {
                            synchronized (metricToClientMap) {
                                // close all clients for this subscriptionId
                                LOG.debug("Channel closed, closing subscriptions for subscriptionId:{}",
                                        currentSubscriptionId);
                                for (Map.Entry<String, WsClientHolder> entry : metricToClientMap.entrySet()) {
                                    entry.getValue().close(wsClientPool);
                                }
                                wsClients.remove(currentSubscriptionId);
                                wsSubscriptions.remove(currentSubscriptionId);
                            }
                        }
                    }
                });

            } else if (msg instanceof AddSubscription) {
                AddSubscription add = (AddSubscription) msg;
                metric = add.getMetric();
                subscriptionId = add.getSubscriptionId();
                origContext = wsSubscriptions.get(subscriptionId);
                if (origContext == null) {
                    // send error because user never sent a CreateSubscription
                    // and we have nowhere to send the results
                    LOG.info("ChannelHandlerContext not found for subscriptionId:{} - createSubscription not called?",
                            subscriptionId);
                    sendErrorResponse(ctx, HttpResponseStatus.BAD_REQUEST,
                            new IllegalArgumentException("Must call create first"));
                }
                Map<String, WsClientHolder> metricToClientMap = wsClients.get(subscriptionId);
                if (metricToClientMap == null) {
                    metricToClientMap = new HashMap<>();
                    wsClients.put(subscriptionId, metricToClientMap);
                }
                WebSocketSubscriptionClient client = null;
                synchronized (metricToClientMap) {
                    WsClientHolder hostClientHolder = metricToClientMap.get(metric);
                    if (hostClientHolder == null) {
                        k = metricResolver.getHostPortKey(metric);
                        client = wsClientPool.borrowObject(k);
                        client.open(new WsClientHandler(origContext, add.getToken(),
                                (balancerConfig.getWebsocket().getTimeout() / 2)));
                        metricToClientMap.put(metric, new WsClientHolder(k, client));
                    } else {
                        client = hostClientHolder.getClient();
                    }
                }
                Map<String, String> tags = null;

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

                client.addSubscription(add.getMetric(), tags, startTime, endTime, delayTime);

            } else if (msg instanceof RemoveSubscription) {
                RemoveSubscription remove = (RemoveSubscription) msg;
                metric = remove.getMetric();
                subscriptionId = remove.getSubscriptionId();
                Map<String, WsClientHolder> metricToClientMap = wsClients.get(subscriptionId);
                if (metricToClientMap != null) {
                    synchronized (metricToClientMap) {
                        WsClientHolder hostClientHolder = metricToClientMap.get(metric);
                        if (hostClientHolder != null) {
                            hostClientHolder.getClient().removeSubscription(metric);
                        } else {
                            LOG.info("client not found for subscriptionId:{} metric:{}", subscriptionId, metric);
                        }
                    }
                }
            } else if (msg instanceof CloseSubscription) {
                CloseSubscription close = (CloseSubscription) msg;
                subscriptionId = close.getSubscriptionId();
                Map<String, WsClientHolder> metricToClientMap = wsClients.get(subscriptionId);
                if (metricToClientMap != null) {
                    synchronized (metricToClientMap) {
                        // close all clients for this subscriptionId
                        for (Map.Entry<String, WsClientHolder> entry : metricToClientMap.entrySet()) {
                            entry.getValue().close(wsClientPool);
                        }
                    }
                    wsClients.remove(subscriptionId);
                    wsSubscriptions.remove(subscriptionId);
                }
                ctx.writeAndFlush(new CloseWebSocketFrame(1000, "Client requested close."));
            }
        } catch (ConnectException e1) {
            LOG.error(e1.getMessage(), e1);
            ChannelHandlerContext errorCtx = origContext == null ? ctx : origContext;
            sendErrorResponse(errorCtx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e1);
        } catch (Exception e2) {
            LOG.error(e2.getMessage(), e2);
            ChannelHandlerContext errorCtx = origContext == null ? ctx : origContext;
            sendErrorResponse(errorCtx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e2);
        }

    }

    static public void sendErrorResponse(ChannelHandlerContext ctx, HttpResponseStatus status, Exception ex) {

        try {
            TimelyException te = new TimelyException(status.code(), ex.getMessage(), ex.getMessage());
            String json = mapper.writeValueAsString(te);
            ctx.writeAndFlush(new TextWebSocketFrame(json));
        } catch (JsonProcessingException e) {
            LOG.error("Error serializing exception");
        }
    }
}

package timely.balancer.netty.ws;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.Session;

import com.google.common.collect.Multimap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.response.TimelyException;
import timely.auth.util.ProxiedEntityUtils;
import timely.client.websocket.ClientHandler;
import timely.netty.http.auth.TimelyAuthenticationToken;

public class WsClientHandler extends ClientHandler {

    private static final Logger LOG = LoggerFactory.getLogger(WsClientHandler.class);
    private final ChannelHandlerContext ctx;
    private final TimelyAuthenticationToken token;
    private ScheduledFuture<?> ping;

    public WsClientHandler(ChannelHandlerContext ctx, TimelyAuthenticationToken token, int pingRate) {
        this.ctx = ctx;
        this.token = token;
        this.ping = this.ctx.executor().scheduleAtFixedRate(() -> {
            LOG.trace("Sending ping on channel {}", ctx.channel());
            ctx.writeAndFlush(new PingWebSocketFrame());
        }, pingRate, pingRate, TimeUnit.SECONDS);
    }

    @Override
    public void beforeRequest(Map<String, List<String>> headers) {
        Multimap<String, String> requestHeaders = token.getHttpHeaders();
        if (token.getClientCert() != null) {
            ProxiedEntityUtils.addProxyHeaders(requestHeaders, token.getClientCert());
        }
        for (String s : requestHeaders.keySet()) {
            List<String> valueList = new ArrayList<>();
            valueList.addAll(requestHeaders.get(s));
            if (headers.containsKey(s)) {
                // add pre-existing values
                valueList.addAll(headers.get(s));
            }
            headers.put(s, valueList);
        }
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        session.addMessageHandler(String.class, message -> {
            ctx.writeAndFlush(new TextWebSocketFrame(message));
            LOG.debug("Message received on Websocket session {}: {}", session.getId(), message);
        });
    }

    @Override
    public void onClose(Session session, CloseReason reason) {
        super.onClose(session, reason);
        if (!reason.getCloseCode().equals(CloseReason.CloseCodes.NORMAL_CLOSURE)) {
            LOG.error("Abnormal close: " + reason.getReasonPhrase());
            Exception e = new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), reason.getReasonPhrase(),
                    "");
            WsRelayHandler.sendErrorResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
        if (!ping.isCancelled()) {
            ping.cancel(false);
        }
    }

    @Override
    public void onError(Session session, Throwable t) {
        super.onError(session, t);
        LOG.error(t.getMessage(), t);
        Exception e = new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), t.getMessage(), "");
        WsRelayHandler.sendErrorResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        if (!ping.isCancelled()) {
            ping.cancel(false);
        }
    }
}

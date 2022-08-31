package timely.balancer.netty.ws;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.ScheduledFuture;
import timely.api.response.TimelyException;
import timely.auth.TimelyAuthenticationToken;
import timely.auth.util.ProxiedEntityUtils;
import timely.client.websocket.ClientHandler;

public class WsClientHandler extends ClientHandler {

    private static final Logger log = LoggerFactory.getLogger(WsClientHandler.class);
    private final ChannelHandlerContext ctx;
    private final TimelyAuthenticationToken token;
    private ScheduledFuture<?> ping;

    public WsClientHandler(ChannelHandlerContext ctx, TimelyAuthenticationToken token, int pingRate) {
        this.ctx = ctx;
        this.token = token;
        this.ping = this.ctx.executor().scheduleAtFixedRate(() -> {
            log.trace("Sending ping on channel {}", ctx.channel());
            ctx.writeAndFlush(new PingWebSocketFrame());
        }, pingRate, pingRate, TimeUnit.SECONDS);
    }

    @Override
    public void beforeRequest(Map<String,List<String>> headers) {
        if (token.getClientCert() != null) {
            Multimap<String,String> proxyRequestHeaders = HashMultimap.create();
            ProxiedEntityUtils.addProxyHeaders(proxyRequestHeaders, token.getClientCert());
            for (String s : proxyRequestHeaders.keySet()) {
                headers.put(s, new ArrayList<>(proxyRequestHeaders.get(s)));
            }
        }

        Multimap<String,String> originalRequestHeaders = token.getHttpHeaders();
        for (String s : originalRequestHeaders.keySet()) {
            if (!headers.containsKey(s)) {
                // add pre-existing values if key does not exist in proxyRequest
                headers.put(s, new ArrayList<>(originalRequestHeaders.get(s)));
            }
        }
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        session.addMessageHandler(String.class, message -> {
            ctx.writeAndFlush(new TextWebSocketFrame(message));
            log.debug("Message received on Websocket session {}: {}", session.getId(), message);
        });
    }

    @Override
    public void onClose(Session session, CloseReason reason) {
        super.onClose(session, reason);
        if (!reason.getCloseCode().equals(CloseReason.CloseCodes.NORMAL_CLOSURE)) {
            log.error("Abnormal close: " + reason.getReasonPhrase());
            Exception e = new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), reason.getReasonPhrase(), "");
            WsRelayHandler.sendErrorResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        }
        if (!ping.isCancelled()) {
            ping.cancel(false);
        }
    }

    @Override
    public void onError(Session session, Throwable t) {
        super.onError(session, t);
        log.error(t.getMessage(), t);
        Exception e = new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), t.getMessage(), "");
        WsRelayHandler.sendErrorResponse(ctx, HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
        if (!ping.isCancelled()) {
            ping.cancel(false);
        }
    }
}

package timely.netty.websocket;

import java.security.cert.X509Certificate;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.FullHttpRequest;
import timely.auth.util.HttpHeaderUtils;
import timely.common.component.AuthenticationService;
import timely.netty.http.HttpRequestDecoder;
import timely.netty.websocket.subscription.SubscriptionConstants;

public class WebSocketFullRequestHandler extends MessageToMessageCodec<FullHttpRequest,FullHttpRequest> {

    private static final Logger log = LoggerFactory.getLogger(WebSocketFullRequestHandler.class);
    private AuthenticationService authenticationService;

    public WebSocketFullRequestHandler(AuthenticationService authenticationService) {
        super();
        this.authenticationService = authenticationService;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
        msg.retain();
        out.add(msg);
        // If the session cookie exists, set its value on the ctx.
        final String sessionId = HttpRequestDecoder.getSessionId(msg);
        if (sessionId != null) {
            ctx.channel().attr(SubscriptionConstants.SESSION_ID_ATTR).set(sessionId);
            log.trace("Found sessionId in WebSocket channel, setting sessionId {} on context", sessionId);
        }
        if (msg.headers() != null) {
            ctx.channel().attr(WebSocketRequestDecoder.HTTP_HEADERS_ATTR).set(HttpHeaderUtils.toMultimap(msg.headers()));
        }

        X509Certificate clientCert = authenticationService.getClientCertificate(ctx);
        if (clientCert != null) {
            ctx.channel().attr(WebSocketRequestDecoder.CLIENT_CERT_ATTR).set(clientCert);
        }
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
        msg.retain();
        out.add(msg);
    }

}

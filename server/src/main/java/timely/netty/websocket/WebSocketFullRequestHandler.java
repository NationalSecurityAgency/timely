package timely.netty.websocket;

import java.security.cert.X509Certificate;
import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageCodec;
import io.netty.handler.codec.http.FullHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.auth.AuthenticationService;
import timely.auth.util.HttpHeaderUtils;
import timely.netty.http.HttpRequestDecoder;
import timely.subscription.SubscriptionRegistry;

public class WebSocketFullRequestHandler extends MessageToMessageCodec<FullHttpRequest, FullHttpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketFullRequestHandler.class);

    public WebSocketFullRequestHandler() {
        super();
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
        msg.retain();
        out.add(msg);
        // If the session cookie exists, set its value on the ctx.
        final String sessionId = HttpRequestDecoder.getSessionId(msg);
        if (sessionId != null) {
            ctx.channel().attr(SubscriptionRegistry.SESSION_ID_ATTR).set(sessionId);
            LOG.trace("Found sessionId in WebSocket channel, setting sessionId {} on context", sessionId);
        }
        if (msg.headers() != null) {
            ctx.channel().attr(WebSocketRequestDecoder.HTTP_HEADERS_ATTR)
                    .set(HttpHeaderUtils.toMultimap(msg.headers()));
        }

        X509Certificate clientCert = AuthenticationService.getClientCertificate(ctx);
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

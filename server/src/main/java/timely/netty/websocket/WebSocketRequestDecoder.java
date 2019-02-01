package timely.netty.websocket;

import java.security.cert.X509Certificate;
import java.util.List;

import com.google.common.collect.Multimap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.ssl.SslCompletionEvent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.WebSocketRequest;
import timely.api.response.TimelyException;
import timely.auth.AuthCache;
import timely.auth.AuthenticationService;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyPrincipal;
import timely.configuration.Security;
import timely.netty.http.auth.TimelyAuthenticationToken;
import timely.subscription.SubscriptionRegistry;
import timely.util.JsonUtil;

public class WebSocketRequestDecoder extends MessageToMessageDecoder<WebSocketFrame> {

    public static final AttributeKey<Multimap<String, String>> HTTP_HEADERS_ATTR = AttributeKey.newInstance("headers");
    public static final AttributeKey<X509Certificate> CLIENT_CERT_ATTR = AttributeKey.newInstance("clientCert");

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketRequestDecoder.class);

    private Security security;

    public WebSocketRequestDecoder(Security security) {
        this.security = security;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, WebSocketFrame msg, List<Object> out) throws Exception {

        if (msg instanceof TextWebSocketFrame) {
            TextWebSocketFrame frame = (TextWebSocketFrame) msg;
            String content = frame.text();
            WebSocketRequest request = JsonUtil.getObjectMapper().readValue(content, WebSocketRequest.class);
            LOG.trace("Received WS request {}", content);
            String sessionId = ctx.channel().attr(SubscriptionRegistry.SESSION_ID_ATTR).get();
            Multimap<String, String> headers = ctx.channel().attr(HTTP_HEADERS_ATTR).get();
            X509Certificate clientCert = ctx.channel().attr(CLIENT_CERT_ATTR).get();
            if (request instanceof AuthenticatedRequest) {
                if (headers != null) {
                    ((AuthenticatedRequest) request).addHeaders(headers);
                }
                TimelyAuthenticationToken token;
                if (StringUtils.isNotBlank(sessionId)) {
                    TimelyPrincipal principal;
                    ((AuthenticatedRequest) request).setSessionId(sessionId);
                    if (AuthCache.getCache().asMap().containsKey(sessionId)) {
                        principal = AuthCache.getCache().asMap().get(sessionId);
                    } else {
                        principal = TimelyPrincipal.anonymousPrincipal();
                    }
                    SubjectIssuerDNPair dn = principal.getPrimaryUser().getDn();
                    token = AuthenticationService.getAuthenticationToken(null, dn.subjectDN(), dn.issuerDN());
                } else if (clientCert != null) {
                    token = AuthenticationService.getAuthenticationToken(clientCert, headers);
                } else {
                    SubjectIssuerDNPair dn = TimelyPrincipal.anonymousPrincipal().getPrimaryUser().getDn();
                    token = AuthenticationService.getAuthenticationToken(null, dn.subjectDN(), dn.issuerDN());
                }
                ((AuthenticatedRequest) request).setToken(token);
            }
            try {
                request.validate();
            } catch (IllegalArgumentException e) {
                LOG.error("Error validating web socket request: " + e.getMessage());
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
                return;
            }
            try {
                if (request instanceof AuthenticatedRequest) {
                    try {
                        AuthenticationService.enforceAccess((AuthenticatedRequest) request);
                    } catch (TimelyException e) {
                        if (!security.isAllowAnonymousWsAccess()) {
                            throw e;
                        }
                    }
                }
            } catch (Exception e) {
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
        } else if (evt instanceof SslCompletionEvent) {
            LOG.debug("{}", ((SslCompletionEvent) evt).getClass().getSimpleName());
        } else if (evt instanceof WebSocketServerProtocolHandler.ServerHandshakeStateEvent) {
            // The handshake completed succesfully and the channel was upgraded to
            // websockets
            LOG.trace("SSL handshake completed successfully, upgraded channel to websockets");
        } else {
            LOG.warn("Received unhandled user event {}", evt);
        }
    }

}

package timely.netty.websocket;

import static timely.common.component.AuthenticationService.AUTH_HEADER;

import java.security.cert.X509Certificate;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import timely.api.request.AuthenticatedRequest;
import timely.api.request.websocket.WebSocketRequest;
import timely.api.response.TimelyException;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyAuthenticationToken;
import timely.auth.TimelyPrincipal;
import timely.auth.util.HttpHeaderUtils;
import timely.common.component.AuthenticationService;
import timely.common.configuration.SecurityProperties;
import timely.netty.websocket.subscription.SubscriptionConstants;
import timely.subscription.SubscriptionRegistry;
import timely.util.JsonUtil;

public class WebSocketRequestDecoder extends MessageToMessageDecoder<WebSocketFrame> {

    public static final AttributeKey<Multimap<String,String>> HTTP_HEADERS_ATTR = AttributeKey.newInstance("headers");
    public static final AttributeKey<X509Certificate> CLIENT_CERT_ATTR = AttributeKey.newInstance("clientCert");

    private static final Logger log = LoggerFactory.getLogger(WebSocketRequestDecoder.class);

    private SecurityProperties securityProperties;
    private AuthenticationService authenticationService;

    public WebSocketRequestDecoder(AuthenticationService authenticationService, SecurityProperties securityProperties) {
        this.authenticationService = authenticationService;
        this.securityProperties = securityProperties;
    }

    @Override
    public void decode(ChannelHandlerContext ctx, WebSocketFrame msg, List<Object> out) throws Exception {

        if (msg instanceof TextWebSocketFrame) {
            TextWebSocketFrame frame = (TextWebSocketFrame) msg;
            String content = frame.text();
            log.trace("Received WS request {}", content);
            WebSocketRequest request = JsonUtil.getObjectMapper().readValue(content, WebSocketRequest.class);
            String sessionId = ctx.channel().attr(SubscriptionConstants.SESSION_ID_ATTR).get();
            Multimap<String,String> headers = ctx.channel().attr(HTTP_HEADERS_ATTR).get();
            X509Certificate clientCert = ctx.channel().attr(CLIENT_CERT_ATTR).get();
            if (request instanceof AuthenticatedRequest) {
                if (headers != null) {
                    ((AuthenticatedRequest) request).addHeaders(headers);
                }
                TimelyAuthenticationToken token;
                String authHeader = HttpHeaderUtils.getSingleHeader(headers, AUTH_HEADER, true);
                if (authHeader != null) {
                    token = authenticationService.getAuthenticationToken(headers);
                } else if (StringUtils.isNotBlank(sessionId)) {
                    TimelyPrincipal principal;
                    ((AuthenticatedRequest) request).setSessionId(sessionId);
                    if (authenticationService.getAuthCache().containsKey(sessionId)) {
                        principal = authenticationService.getAuthCache().get(sessionId);
                    } else {
                        principal = TimelyPrincipal.anonymousPrincipal();
                    }
                    SubjectIssuerDNPair dn = principal.getPrimaryUser().getDn();
                    token = authenticationService.getAuthenticationToken(null, dn.subjectDN(), dn.issuerDN());
                } else if (clientCert != null) {
                    token = authenticationService.getAuthenticationToken(clientCert, headers);
                } else {
                    SubjectIssuerDNPair dn = TimelyPrincipal.anonymousPrincipal().getPrimaryUser().getDn();
                    token = authenticationService.getAuthenticationToken(null, dn.subjectDN(), dn.issuerDN());
                }
                ((AuthenticatedRequest) request).setToken(token);
            }
            try {
                request.validate();
            } catch (IllegalArgumentException e) {
                log.error("Error validating web socket request: " + e.getMessage());
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
                return;
            }
            try {
                if (request instanceof AuthenticatedRequest) {
                    try {
                        authenticationService.enforceAccess((AuthenticatedRequest) request);
                    } catch (TimelyException e) {
                        if (!securityProperties.isAllowAnonymousWsAccess()) {
                            throw e;
                        }
                    }
                }
            } catch (Exception e) {
                out.clear();
                log.error("Error during access enforcment: " + e.getMessage());
                ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
                return;
            }
            out.add(request);

        } else {
            log.error("Unhandled web socket frame type");
            ctx.writeAndFlush(new CloseWebSocketFrame(1003, "Unhandled web socket frame type, only TextWebSocketFrame is supported"));
        }
    }

    public static void close() {
        SubscriptionRegistry.get().forEach((k, v) -> v.close());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("Error caught", cause);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleStateEvent idle = (IdleStateEvent) evt;
            if (idle.state() == IdleState.READER_IDLE) {
                // We have not read any data from client in a while, let's close
                // the subscriptions for this context.
                String subscriptionId = ctx.channel().attr(SubscriptionConstants.SUBSCRIPTION_ID_ATTR).get();
                if (!StringUtils.isEmpty(subscriptionId)) {
                    if (SubscriptionRegistry.get().containsKey(subscriptionId)) {
                        log.info("Closing subscription with subscription id {} due to idle event", subscriptionId);
                        SubscriptionRegistry.get().get(subscriptionId).close();
                    }
                } else {
                    log.warn("Channel idle, but no subscription id found on context. Unable to close subscriptions");
                }
            }
        } else if (evt instanceof SslCompletionEvent) {
            log.debug("{}", ((SslCompletionEvent) evt).getClass().getSimpleName());
        } else if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            // The handshake completed succesfully and the channel was upgraded to
            // websockets
            log.trace("SSL handshake completed successfully, upgraded channel to websockets");
        } else {
            log.warn("Received unhandled user event {}", evt);
        }
    }

}

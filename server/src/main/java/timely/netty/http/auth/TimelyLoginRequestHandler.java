package timely.netty.http.auth;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import timely.auth.TimelyPrincipal;
import timely.configuration.Http;
import timely.configuration.Security;
import timely.netty.Constants;
import timely.netty.http.TimelyHttpHandler;

public abstract class TimelyLoginRequestHandler<T> extends SimpleChannelInboundHandler<T> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyLoginRequestHandler.class);

    private final long maxAge;
    private final String domain;

    public TimelyLoginRequestHandler(Security security, Http http) {
        maxAge = security.getSessionMaxAge();
        domain = http.getHost();
    }

    @Override
    protected final void channelRead0(ChannelHandlerContext ctx, T loginRequest) throws Exception {
        try {
            LOG.trace("Authenticating {}", loginRequest);
            String sessionId = UUID.randomUUID().toString();
            TimelyPrincipal principal = authenticate(ctx, loginRequest, sessionId);
            LOG.trace("Authenticated new sessionId {} for user {}", sessionId, principal.getPrimaryUser().getDn().subjectDN());
            String sessionIdEncoded = URLEncoder.encode(sessionId, StandardCharsets.UTF_8.name());
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, Constants.JSON_TYPE);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            DefaultCookie cookie = new DefaultCookie(Constants.COOKIE_NAME, sessionIdEncoded);
            cookie.setDomain(domain);
            cookie.setMaxAge(maxAge);
            cookie.setPath("/");
            cookie.setHttpOnly(true);
            cookie.setSecure(true);
            response.headers().set(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
            sendResponse(ctx, response);
        } catch (Exception e) {
            LOG.error("Login failure", e);
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.UNAUTHORIZED);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, Constants.JSON_TYPE);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            sendResponse(ctx, response);
        }

    }

    protected abstract TimelyPrincipal authenticate(ChannelHandlerContext ctx, T loginRequest, String entity) throws Exception;
}

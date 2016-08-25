package timely.netty.http.auth;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;

import timely.Configuration;
import timely.auth.AuthCache;
import timely.netty.Constants;
import timely.netty.http.TimelyHttpHandler;

public abstract class TimelyLoginRequestHandler<T> extends SimpleChannelInboundHandler<T> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyLoginRequestHandler.class);

    private final long maxAge;
    private final String domain;

    public TimelyLoginRequestHandler(Configuration conf) {
        maxAge = conf.getSecurity().getSessionMaxAge();
        domain = conf.getHttp().getHost();
    }

    @Override
    protected final void channelRead0(ChannelHandlerContext ctx, T loginRequest) throws Exception {
        try {
            LOG.trace("Authenticating {}", loginRequest);
            Authentication auth = authenticate(ctx, loginRequest);
            LOG.trace("Authenticated {}", auth);
            String sessionId = URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name());
            AuthCache.getCache().put(sessionId, auth);
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            response.headers().set(Names.CONTENT_TYPE, Constants.JSON_TYPE);
            response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
            DefaultCookie cookie = new DefaultCookie(Constants.COOKIE_NAME, sessionId);
            cookie.setDomain(domain);
            cookie.setMaxAge(maxAge);
            cookie.setPath("/");
            cookie.setHttpOnly(true);
            cookie.setSecure(true);
            response.headers().set(Names.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
            sendResponse(ctx, response);
        } catch (Exception e) {
            LOG.error("Login failure", e);
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.UNAUTHORIZED);
            response.headers().set(Names.CONTENT_TYPE, Constants.JSON_TYPE);
            response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
            sendResponse(ctx, response);
        }

    }

    protected abstract Authentication authenticate(ChannelHandlerContext ctx, T loginRequest) throws Exception;
}

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

    private long maxAge = Configuration.SESSION_MAX_AGE_DEFAULT;
    private String domain = null;
    private String redirect = null;

    public TimelyLoginRequestHandler(Configuration conf) {
        String ma = conf.get(Configuration.SESSION_MAX_AGE);
        if (null != ma) {
            maxAge = Long.parseLong(ma);
        }
        domain = conf.get(Configuration.TIMELY_HTTP_HOST);
        redirect = conf.get(Configuration.GRAFANA_HTTP_ADDRESS);
    }

    @Override
    protected final void channelRead0(ChannelHandlerContext ctx, T loginRequest) throws Exception {
        try {
            LOG.trace("Authenticating {}", loginRequest);
            Authentication auth = authenticate(ctx, loginRequest);
            LOG.trace("Authenticated {}", auth);
            String sessionId = URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name());
            AuthCache.getCache().put(sessionId, auth);
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.TEMPORARY_REDIRECT);
            response.headers().set(Names.CONTENT_TYPE, Constants.JSON_TYPE);
            response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
            response.headers().set(Names.LOCATION, redirect);
            DefaultCookie cookie = new DefaultCookie(Constants.COOKIE_NAME, sessionId);
            cookie.setDomain(domain);
            cookie.setMaxAge(maxAge);
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

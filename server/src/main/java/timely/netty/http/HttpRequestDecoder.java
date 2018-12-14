package timely.netty.http;

import java.nio.charset.StandardCharsets;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.annotation.AnnotationResolver;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.timeseries.HttpRequest;
import timely.api.response.StrictTransportResponse;
import timely.api.response.TimelyException;
import timely.auth.AuthCache;
import timely.configuration.Http;
import timely.configuration.Security;
import timely.netty.Constants;

public class HttpRequestDecoder extends MessageToMessageDecoder<FullHttpRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestDecoder.class);
    private static final String LOG_RECEIVED_REQUEST = "Received HTTP request {}";
    private static final String LOG_PARSED_REQUEST = "Parsed request {}";
    private static final String NO_AUTHORIZATIONS = "";

    private final Security security;
    private final String nonSecureRedirectAddress;

    public HttpRequestDecoder(Security security, Http http) {

        this.security = security;
        this.nonSecureRedirectAddress = http.getRedirectPath();
    }

    public static String getSessionId(FullHttpRequest msg, boolean anonymousAccessAllowed) {
        final StringBuilder buf = new StringBuilder();
        msg.headers().getAll(Names.COOKIE).forEach(h -> {
            ServerCookieDecoder.STRICT.decode(h).forEach(c -> {
                if (c.name().equals(Constants.COOKIE_NAME)) {
                    if (buf.length() == 0) {
                        buf.append(c.value());
                    }
                }
            });
        });
        String sessionId = buf.toString();
        if (sessionId.length() == 0 && anonymousAccessAllowed) {
            sessionId = NO_AUTHORIZATIONS;
        } else if (sessionId.length() == 0) {
            sessionId = null;
        }
        return sessionId;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {

        LOG.trace(LOG_RECEIVED_REQUEST, msg);

        final String uri = msg.getUri();
        final QueryStringDecoder decoder = new QueryStringDecoder(uri);
        if (decoder.path().equals(nonSecureRedirectAddress)) {
            out.add(new StrictTransportResponse());
            return;
        }

        final String sessionId = getSessionId(msg, this.security.isAllowAnonymousAccess());
        LOG.trace("SessionID: " + sessionId);

        HttpRequest request;
        try {
            if (msg.getMethod().equals(HttpMethod.GET)) {
                HttpGetRequest get = AnnotationResolver.getClassForHttpGet(decoder.path());
                request = get.parseQueryParameters(decoder);
            } else if (msg.getMethod().equals(HttpMethod.POST)) {
                HttpPostRequest post = AnnotationResolver.getClassForHttpPost(decoder.path());
                String content = "";
                ByteBuf body = msg.content();
                if (null != body) {
                    content = body.toString(StandardCharsets.UTF_8);
                }
                request = post.parseBody(content);
            } else {
                TimelyException e = new TimelyException(HttpResponseStatus.METHOD_NOT_ALLOWED.code(),
                        "unhandled method type", "");
                e.addResponseHeader(Names.ALLOW, HttpMethod.GET.name() + "," + HttpMethod.POST.name());
                LOG.warn("Unhandled HTTP request type {}", msg.getMethod());
                throw e;
            }
            if (request instanceof AuthenticatedRequest && sessionId != null) {
                ((AuthenticatedRequest) request).setSessionId(sessionId);
                ((AuthenticatedRequest) request).addHeaders(msg.headers().entries());
            }
            request.setHttpRequest(msg.copy());
            LOG.trace(LOG_PARSED_REQUEST, request);
            request.validate();
            out.add(request);
        } catch (UnsupportedOperationException | NullPointerException e) {
            // Return the original http request to route to the static file
            // server
            msg.retain();
            out.add(msg);
            return;
        }
        try {
            AuthCache.enforceAccess(security, request);
        } catch (Exception e) {
            out.clear();
            throw e;
        }

    }

}

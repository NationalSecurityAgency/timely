package timely.netty.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.api.annotation.AnnotationResolver;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.Request;
import timely.api.response.StrictTransportResponse;
import timely.api.response.TimelyException;
import timely.auth.AuthCache;
import timely.netty.Constants;

public class HttpRequestDecoder extends MessageToMessageDecoder<FullHttpRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRequestDecoder.class);
    private static final String LOG_RECEIVED_REQUEST = "Received HTTP request {}";
    private static final String LOG_PARSED_REQUEST = "Parsed request {}";
    private static final String NO_AUTHORIZATIONS = "";

    private final Configuration conf;
    private boolean anonymousAccessAllowed = false;
    private final String nonSecureRedirectAddress;

    public HttpRequestDecoder(Configuration config) {
        this.conf = config;
        this.anonymousAccessAllowed = conf.getBoolean(Configuration.ALLOW_ANONYMOUS_ACCESS);
        this.nonSecureRedirectAddress = conf.get(Configuration.NON_SECURE_REDIRECT_PATH);
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

        String sId = buf.toString();
        if (sId.length() == 0 && this.anonymousAccessAllowed) {
            sId = NO_AUTHORIZATIONS;
        } else if (sId.length() == 0) {
            sId = null;
        }
        final String sessionId = sId;
        LOG.trace("SessionID: " + sessionId);

        Request request = null;
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
            LOG.trace(LOG_PARSED_REQUEST, request);
            request.validate();
            out.add(request);
        } catch (UnsupportedOperationException | NullPointerException e) {
            // Return the original http request to route to the static file
            // server
            LOG.error("Error decoding request: " + msg, e);
            msg.retain();
            out.add(msg);
            return;
        }
        try {
            AuthCache.enforceAccess(conf, request);
        } catch (Exception e) {
            out.clear();
            throw e;
        }

    }

}

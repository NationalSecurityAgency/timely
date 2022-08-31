package timely.netty.http;

import static timely.common.component.AuthenticationService.AUTH_HEADER;

import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import timely.api.annotation.AnnotationResolver;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.HttpRequest;
import timely.api.response.StrictTransportResponse;
import timely.api.response.TimelyException;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyAuthenticationToken;
import timely.auth.TimelyPrincipal;
import timely.auth.util.HttpHeaderUtils;
import timely.common.component.AuthenticationService;
import timely.common.configuration.HttpProperties;
import timely.common.configuration.SecurityProperties;
import timely.netty.Constants;

public class HttpRequestDecoder extends MessageToMessageDecoder<FullHttpRequest> implements TimelyHttpHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpRequestDecoder.class);
    private static final String LOG_RECEIVED_REQUEST = "Received HTTP request {}";
    private static final String LOG_PARSED_REQUEST = "Parsed request {}";

    protected SecurityProperties securityProperties;
    private final String nonSecureRedirectAddress;
    private AuthenticationService authenticationService;

    public HttpRequestDecoder(AuthenticationService authenticationService, SecurityProperties securityProperties, HttpProperties httpProperties) {

        this.securityProperties = securityProperties;
        this.nonSecureRedirectAddress = httpProperties.getRedirectPath();
        this.authenticationService = authenticationService;
    }

    public static String getSessionId(FullHttpRequest msg) {
        Multimap<String,String> headers = HttpHeaderUtils.toMultimap(msg.headers());
        Collection<String> cookies = headers.get(HttpHeaderNames.COOKIE.toString());
        final StringBuilder buf = new StringBuilder();
        cookies.forEach(h -> {
            ServerCookieDecoder.STRICT.decode(h).forEach(c -> {
                if (c.name().equals(Constants.COOKIE_NAME)) {
                    if (buf.length() == 0) {
                        buf.append(c.value());
                    }
                }
            });
        });
        if (buf.length() == 0) {
            return null;
        } else {
            return buf.toString();
        }
    }

    @Override
    public void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {

        log.trace(LOG_RECEIVED_REQUEST, msg);

        final String uri = msg.uri();
        final QueryStringDecoder decoder = new QueryStringDecoder(uri);
        if (decoder.path().equals(nonSecureRedirectAddress)) {
            out.add(new StrictTransportResponse());
            return;
        }

        final String sessionId = getSessionId(msg);
        log.trace("SessionID: " + sessionId);

        HttpRequest request;
        try {
            if (msg.method().equals(HttpMethod.GET)) {
                HttpGetRequest get = AnnotationResolver.getClassForHttpGet(decoder.path());
                request = get.parseQueryParameters(decoder);
            } else if (msg.method().equals(HttpMethod.POST)) {
                HttpPostRequest post = AnnotationResolver.getClassForHttpPost(decoder.path());
                String content = "";
                ByteBuf body = msg.content();
                if (null != body) {
                    content = body.toString(StandardCharsets.UTF_8);
                }
                request = post.parseBody(content);
            } else {
                TimelyException e = new TimelyException(HttpResponseStatus.METHOD_NOT_ALLOWED.code(), "unhandled method type", "");
                e.addResponseHeader(HttpHeaderNames.ALLOW.toString(), HttpMethod.GET.name() + "," + HttpMethod.POST.name());
                log.warn("Unhandled HTTP request type {}", msg.method());
                throw e;
            }
            if (request instanceof AuthenticatedRequest) {
                Multimap<String,String> headers = HttpHeaderUtils.toMultimap(msg.headers());
                ((AuthenticatedRequest) request).addHeaders(headers);
                X509Certificate clientCert = authenticationService.getClientCertificate(ctx);
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
            request.setHttpRequest(msg.retain());
            log.trace(LOG_PARSED_REQUEST, request);
            request.validate();
            out.add(request);
        } catch (UnsupportedOperationException | NullPointerException e) {
            // Return the original http request to route to the static file server
            out.add(msg.retain());
            return;
        }
        try {
            if (request instanceof AuthenticatedRequest) {
                try {
                    authenticationService.enforceAccess((AuthenticatedRequest) request);
                } catch (TimelyException e) {
                    if (!securityProperties.isAllowAnonymousHttpAccess()) {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            out.clear();
            // route to TimelyExceptionHandler
            out.add(e);
        }

    }

}

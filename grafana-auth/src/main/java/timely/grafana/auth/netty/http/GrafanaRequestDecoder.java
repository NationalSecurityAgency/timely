package timely.grafana.auth.netty.http;

import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.Multimap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.timeseries.HttpRequest;
import timely.api.response.StrictTransportResponse;
import timely.api.response.TimelyException;
import timely.auth.AuthCache;
import timely.auth.AuthenticationService;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyPrincipal;
import timely.auth.util.HttpHeaderUtils;
import timely.configuration.Http;
import timely.configuration.Security;
import timely.grafana.request.GrafanaRequest;
import timely.netty.Constants;
import timely.netty.http.TimelyHttpHandler;
import timely.netty.http.auth.TimelyAuthenticationToken;

public class GrafanaRequestDecoder extends MessageToMessageDecoder<FullHttpRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GrafanaRequestDecoder.class);
    private static final String LOG_RECEIVED_REQUEST = "Received HTTP request {}";
    private static final String LOG_PARSED_REQUEST = "Parsed request {}";

    private final Security security;
    private final String nonSecureRedirectAddress;

    public GrafanaRequestDecoder(Security security, Http http) {

        this.security = security;
        this.nonSecureRedirectAddress = http.getRedirectPath();
    }

    public static String getSessionId(FullHttpRequest msg) {
        Multimap<String, String> headers = HttpHeaderUtils.toMultimap(msg.headers());
        Collection<String> cookies = headers.get(Names.COOKIE);
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
    protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {

        LOG.trace(LOG_RECEIVED_REQUEST, msg);

        final String uri = msg.getUri();
        final QueryStringDecoder decoder = new QueryStringDecoder(uri);
        if (decoder.path().equals(nonSecureRedirectAddress)) {
            out.add(new StrictTransportResponse());
            return;
        }

        final String sessionId = getSessionId(msg);
        LOG.trace("SessionID: " + sessionId);

        HttpRequest request;
        try {
            request = new GrafanaRequest();
            Multimap<String, String> headers = HttpHeaderUtils.toMultimap(msg.headers());
            ((AuthenticatedRequest) request).addHeaders(headers);
            X509Certificate clientCert = AuthenticationService.getClientCertificate(ctx);
            TimelyAuthenticationToken token;
            if (StringUtils.isNotBlank(sessionId)) {
                TimelyPrincipal principal;
                ((AuthenticatedRequest) request).setSessionId(sessionId);
                if (AuthCache.containsKey(sessionId)) {
                    principal = AuthCache.get(sessionId);
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
            try {
                AuthenticationService.enforceAccess((AuthenticatedRequest) request);
            } catch (TimelyException e) {
                if (!security.isAllowAnonymousHttpAccess()) {
                    throw e;
                }
            }
        } catch (Exception e) {
            out.clear();
            throw e;
        }

    }

}

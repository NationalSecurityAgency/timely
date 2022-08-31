package timely.netty.http.auth;

import java.security.cert.X509Certificate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import timely.api.request.auth.X509LoginRequest;
import timely.api.response.TimelyException;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyAuthenticationToken;
import timely.auth.TimelyPrincipal;
import timely.common.component.AuthenticationService;
import timely.common.configuration.HttpProperties;
import timely.common.configuration.SecurityProperties;

public class X509LoginRequestHandler extends TimelyLoginRequestHandler<X509LoginRequest> {

    private static final Logger log = LoggerFactory.getLogger(X509LoginRequestHandler.class);

    public X509LoginRequestHandler(AuthenticationService authenticationService, SecurityProperties securityProperties, HttpProperties httpProperties) {
        super(authenticationService, securityProperties, httpProperties);
    }

    @Override
    protected TimelyPrincipal authenticate(ChannelHandlerContext ctx, X509LoginRequest loginRequest, String sessionId) throws Exception {
        // If we are operating in 2 way SSL, then get the subjectDN from the
        // client certificate and perform the login process.
        TimelyPrincipal principal;
        try {
            X509Certificate clientCert = authenticationService.getClientCertificate(ctx);
            if (clientCert == null) {
                throw new IllegalArgumentException("No client certificate found");
            } else {
                // login endpoint is for direct certificate only, not for proxying
                TimelyAuthenticationToken token = authenticationService.getAuthenticationToken(clientCert, clientCert.getSubjectDN().getName(),
                                clientCert.getIssuerDN().getName());
                SubjectIssuerDNPair pair = SubjectIssuerDNPair.of(clientCert.getSubjectDN().getName(), clientCert.getIssuerDN().getName());
                principal = authenticationService.authenticate(token, pair, sessionId);
                log.debug("Authenticated user {} with authorizations {}", principal.getPrimaryUser().getDn().subjectDN(), principal.getAuthorizationsString());
            }
        } catch (Exception e) {
            throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "Error performing login", e.getMessage(), e);
        }
        return principal;
    }

}

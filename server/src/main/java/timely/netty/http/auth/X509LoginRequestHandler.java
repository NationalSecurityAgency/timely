package timely.netty.http.auth;

import java.security.cert.X509Certificate;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.auth.X509LoginRequest;
import timely.api.response.TimelyException;
import timely.auth.AuthenticationService;
import timely.auth.TimelyPrincipal;
import timely.configuration.Http;
import timely.configuration.Security;

public class X509LoginRequestHandler extends TimelyLoginRequestHandler<X509LoginRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(X509LoginRequestHandler.class);

    public X509LoginRequestHandler(Security security, Http http) {
        super(security, http);
    }

    @Override
    protected TimelyPrincipal authenticate(ChannelHandlerContext ctx, X509LoginRequest loginRequest, String sessionId)
            throws Exception {
        // If we are operating in 2 way SSL, then get the subjectDN from the
        // client certificate and perform the login process.
        TimelyPrincipal principal = null;
        try {
            X509Certificate clientCert = AuthenticationService.getClientCertificate(ctx);
            if (clientCert == null) {
                throw new IllegalArgumentException("No client certificate found");
            } else {
                // login endpoint is for direct certificate only, not for proxying
                TimelyAuthenticationToken token = AuthenticationService.getAuthenticationToken(clientCert,
                        clientCert.getSubjectDN().getName(), clientCert.getIssuerDN().getName());
                principal = AuthenticationService.authenticate(token, clientCert, sessionId);
                LOG.debug("Authenticated user {} with authorizations {}",
                        principal.getPrimaryUser().getDn().subjectDN(), principal.getAuthorizationsString());
            }
        } catch (Exception e) {
            throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "Error performing login", e.getMessage(),
                    e);
        }
        return principal;
    }

}

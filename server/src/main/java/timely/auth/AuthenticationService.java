package timely.auth;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceConfigurationError;

import com.google.common.collect.Multimap;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.preauth.x509.SubjectDnX509PrincipalExtractor;
import timely.api.request.AuthenticatedRequest;
import timely.api.response.TimelyException;
import timely.auth.util.AuthenticationUtils;
import timely.netty.http.auth.TimelyAuthenticationToken;

public class AuthenticationService {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationService.class);
    private static ApplicationContext springContext = null;
    private static AuthenticationManager authManager = null;
    private static SubjectDnX509PrincipalExtractor x509 = null;

    static {
        try {
            springContext = new ClassPathXmlApplicationContext("security.xml");
            authManager = (AuthenticationManager) springContext.getBean("authenticationManager");
            x509 = (SubjectDnX509PrincipalExtractor) springContext.getBean("x509PrincipalExtractor");
        } catch (BeansException e) {
            throw new ServiceConfigurationError("Error setting up Authentication objects: " + e.getMessage(),
                    e.getRootCause());
        }
    }

    private static AuthenticationManager getAuthenticationManager() {
        return authManager;
    }

    public static TimelyAuthenticationToken authenticate(Authentication authentication) {
        return authenticate(authentication, null);
    }

    public static TimelyAuthenticationToken authenticate(Authentication authentication, X509Certificate clientCert) {
        Authentication token = getAuthenticationManager().authenticate(authentication);
        return AuthenticationUtils.getTimelyAuthentication(token, clientCert);
    }

    public static void enforceAccess(AuthenticatedRequest request) throws Exception {
        String sessionId = request.getSessionId();
        X509Certificate clientCert = request.getToken().getClientCert();
        if (StringUtils.isBlank(sessionId) && clientCert == null) {
            throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must log in",
                    "Anonymous access is disabled.  User must either send a client certificate or log in");
        } else if (StringUtils.isNotBlank(sessionId)) {
            if (!AuthCache.getCache().asMap().containsKey(sessionId)) {
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must log in",
                        "Unknown session id was submitted, log in again");
            }
        } else {
            try {
                TimelyAuthenticationToken token = request.getToken();
                TimelyPrincipal requestPrincipal = token.getTimelyPrincipal();
                if (!AuthCache.getCache().asMap().containsKey(requestPrincipal.getName())) {
                    List<TimelyUser> authenticatedTimelyUsers = new ArrayList<>();
                    for (TimelyUser user : requestPrincipal.getProxiedUsers()) {
                        SubjectIssuerDNPair p = user.getDn();
                        TimelyAuthenticationToken entityToken = getAuthenticationToken(clientCert, p.subjectDN(),
                                p.issuerDN());
                        TimelyPrincipal entityPrincipal = AuthCache.getCache().asMap()
                                .get(entityToken.getTimelyPrincipal().getName());
                        if (entityPrincipal == null) {
                            TimelyAuthenticationToken t = authenticate(entityToken, clientCert);
                            entityPrincipal = t.getTimelyPrincipal();
                            AuthCache.getCache().put(entityPrincipal.getName(), entityPrincipal);
                        }
                        authenticatedTimelyUsers.addAll(entityPrincipal.getProxiedUsers());
                    }
                    TimelyPrincipal principal = new TimelyPrincipal(authenticatedTimelyUsers);
                    AuthCache.getCache().put(principal.getName(), principal);
                    LOG.debug("Authenticated user {} for request {} with authorizations {}", principal.getName(),
                            request.toString(), principal.getAuthorizationsString());
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "Access denied", e.getMessage());
            }
        }
    }

    public static X509Certificate getClientCertificate(ChannelHandlerContext ctx) {
        X509Certificate clientCert = null;
        if (ctx != null) {
            SslHandler sslHandler = (SslHandler) ctx.channel().pipeline().get("ssl");
            if (null != sslHandler) {
                try {
                    clientCert = (X509Certificate) sslHandler.engine().getSession().getPeerCertificates()[0];
                } catch (Exception e) {

                }
            } else {
                throw new IllegalStateException("The expected SSL handler is not in the pipeline.");
            }
        }
        return clientCert;
    }

    public static TimelyAuthenticationToken getAuthenticationToken(X509Certificate clientCert,
            Multimap<String, String> headers) {
        TimelyAuthenticationToken authenticationToken = null;
        try {
            authenticationToken = new TimelyAuthenticationToken(clientCert.getSubjectDN().getName(), clientCert,
                    headers);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return authenticationToken;
    }

    public static TimelyAuthenticationToken getAuthenticationToken(X509Certificate clientCert, String subjectDn,
            String issuerDn) {
        TimelyAuthenticationToken authenticationToken = null;
        try {
            authenticationToken = new TimelyAuthenticationToken(subjectDn, issuerDn, clientCert);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return authenticationToken;
    }
}

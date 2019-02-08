package timely.auth;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
import timely.auth.util.DnUtils;
import timely.netty.http.auth.TimelyAuthenticationToken;

public class AuthenticationService {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationService.class);
    private static ApplicationContext springContext = null;
    private static AuthenticationManager authManager = null;
    private static SubjectDnX509PrincipalExtractor x509 = null;
    private static ConcurrentHashMap<String, Object> fetchingEntity = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Object> fetchingPrincipal = new ConcurrentHashMap<>();

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

    public static TimelyPrincipal authenticate(Authentication authentication, String cacheName) {
        return authenticate(authentication, SubjectIssuerDNPair.of(cacheName), cacheName);
    }

    public static TimelyPrincipal authenticate(Authentication authentication, SubjectIssuerDNPair pair, String entity) {

        // only one thread should call the authenticationManager for a given entity
        TimelyPrincipal principal = AuthCache.get(entity);
        // once an entity is in the AuthCache, we can bypass all of this
        if (principal == null) {
            Object newObj = new Object();
            Object o = fetchingEntity.getOrDefault(entity, newObj);
            if (o == null) {
                o = newObj;
            }
            try {
                synchronized (o) {
                    // if multiple threads are waiting on this synchronized, then the 2nd and all
                    // others will get the principal here
                    principal = AuthCache.get(entity);
                    if (principal == null) {
                        Authentication token = getAuthenticationManager().authenticate(authentication);
                        principal = getTimelyPrincipal(token, pair);
                        LOG.trace("Got entity principal for {} from AuthenticationManager", entity);
                        AuthCache.put(entity, principal);
                    } else {
                        LOG.trace("Got entity principal for {} from AuthCache on 2nd attempt", entity);
                    }
                }
            } finally {
                fetchingEntity.remove(entity);
            }
        } else {
            LOG.trace("Got entity principal for {} from AuthCache on 1st attempt", entity);
        }
        return principal;
    }

    public static void enforceAccess(AuthenticatedRequest request) throws Exception {
        String sessionId = request.getSessionId();
        X509Certificate clientCert = request.getToken().getClientCert();
        if (StringUtils.isBlank(sessionId) && clientCert == null) {
            throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must log in",
                    "Anonymous access is disabled.  User must either send a client certificate or log in");
        } else if (StringUtils.isNotBlank(sessionId)) {
            if (!AuthCache.containsKey(sessionId)) {
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must log in",
                        "Unknown session id was submitted, log in again");
            }
        } else {
            try {
                TimelyAuthenticationToken token = request.getToken();
                TimelyPrincipal requestPrincipal = token.getTimelyPrincipal();
                String entity = requestPrincipal.getName();
                if (!AuthCache.containsKey(requestPrincipal.getName())) {
                    Object newObj = new Object();
                    Object o = fetchingPrincipal.getOrDefault(entity, newObj);
                    if (o == null) {
                        o = newObj;
                    }
                    try {
                        synchronized (o) {
                            // check again if in the AuthCache
                            if (!AuthCache.containsKey(entity)) {
                                // first thread here, so authenticate all entities in requestPrincipal
                                List<TimelyUser> authenticatedTimelyUsers = new ArrayList<>();
                                for (TimelyUser user : requestPrincipal.getProxiedUsers()) {
                                    SubjectIssuerDNPair p = user.getDn();
                                    TimelyAuthenticationToken entityToken = getAuthenticationToken(clientCert,
                                            p.subjectDN(), p.issuerDN());
                                    TimelyPrincipal entityPrincipal = authenticate(entityToken, p,
                                            entityToken.getTimelyPrincipal().getName());
                                    authenticatedTimelyUsers.addAll(entityPrincipal.getProxiedUsers());
                                }
                                TimelyPrincipal principal = new TimelyPrincipal(authenticatedTimelyUsers);
                                AuthCache.put(principal.getName(), principal);
                                LOG.debug("Authenticated user {} for request {} with authorizations {}",
                                        principal.getName(), request.toString(), principal.getAuthorizationsString());
                            } else {
                                LOG.trace("Verified principal {} in AuthCache on 2nd attempt", entity);
                            }
                        }
                    } finally {
                        fetchingPrincipal.remove(entity);
                    }
                } else {
                    LOG.trace("Verified principal {} in AuthCache on 1st attempt", entity);
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

    private static TimelyPrincipal getTimelyPrincipal(Authentication authentication,
            SubjectIssuerDNPair subjectIssuerDNPair) {
        String subjectDn = subjectIssuerDNPair.subjectDN();
        TimelyUser.UserType userType = DnUtils.isServerDN(subjectDn) ? TimelyUser.UserType.SERVER
                : TimelyUser.UserType.USER;
        Collection<String> auths = authentication.getAuthorities().stream().map(a -> a.getAuthority())
                .collect(Collectors.toList());
        TimelyUser timelyUser = new TimelyUser(subjectIssuerDNPair, userType, auths, null, null,
                System.currentTimeMillis());
        return new TimelyPrincipal(Arrays.asList(timelyUser));
    }
}

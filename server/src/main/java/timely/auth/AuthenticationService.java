package timely.auth;

import java.security.cert.X509Certificate;
import java.util.*;
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
import timely.api.request.AuthenticatedRequest;
import timely.api.response.TimelyException;
import timely.auth.util.DnUtils;
import timely.auth.util.HttpHeaderUtils;
import timely.netty.http.auth.TimelyAuthenticationToken;
import timely.netty.http.auth.TimelyUserDetails;

public class AuthenticationService {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationService.class);
    private static ApplicationContext springContext = null;
    private static AuthenticationManager authManager = null;
    private static ConcurrentHashMap<String, Object> fetchingEntity = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Object> fetchingPrincipal = new ConcurrentHashMap<>();
    private static Collection<String> requiredRoles;
    private static Collection<String> requiredAuths;
    public static final String AUTH_HEADER = "Authorization";
    public static final String PRINCIPALS_CLAIM = "principals";

    static {
        try {
            springContext = new ClassPathXmlApplicationContext("security.xml");
            authManager = springContext.getBean("authenticationManager", AuthenticationManager.class);
            @SuppressWarnings("unchecked")
            List<String> uncheckedRoles = springContext.getBean("requiredRoles", List.class);
            requiredRoles = uncheckedRoles;
            requiredRoles.removeIf(String::isEmpty);
            @SuppressWarnings("unchecked")
            List<String> uncheckedAuths = springContext.getBean("requiredAuths", List.class);
            requiredAuths = uncheckedAuths;
            requiredAuths.removeIf(String::isEmpty);
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
        return authenticate(authentication, pair, entity, true);
    }

    public static TimelyPrincipal authenticate(Authentication authentication, SubjectIssuerDNPair pair, String entity,
            boolean useCache) {

        // only one thread should call the authenticationManager for a given entity
        TimelyPrincipal principal = null;
        if (useCache) {
            principal = AuthCache.get(entity);
        }
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
                    if (useCache) {
                        principal = AuthCache.get(entity);
                    }
                    if (principal == null) {
                        Authentication token = getAuthenticationManager().authenticate(authentication);
                        principal = getTimelyPrincipal(token, pair);
                        LOG.trace("Got entity principal for {} from AuthenticationManager", entity);
                        if (useCache) {
                            AuthCache.put(entity, principal);
                        }
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
        TimelyPrincipal verifiedPrincipal;
        String oauthToken = request.getRequestHeader(AUTH_HEADER);
        if (StringUtils.isBlank(sessionId) && StringUtils.isBlank(oauthToken) && clientCert == null) {
            throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must authenticate",
                    "User must authenticate with a client certificate, OAuth token, or login credentials");
        } else if (StringUtils.isNotBlank(sessionId)) {
            verifiedPrincipal = AuthCache.get(sessionId);
            if (verifiedPrincipal == null) {
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must authenticate",
                        "Unknown session id was submitted, log in again");
            }
        } else {
            try {
                TimelyAuthenticationToken token = request.getToken();
                TimelyPrincipal requestPrincipal = token.getTimelyPrincipal();
                String entity = requestPrincipal.getName();
                if (oauthToken != null) {
                    // if the requestPrincipal came from an oauthToken, use that
                    TimelyPrincipal cachedPrincipal = AuthCache.get(entity);
                    if (cachedPrincipal == null
                            || (cachedPrincipal.getCreationTime() != requestPrincipal.getCreationTime())) {
                        AuthCache.put(entity, requestPrincipal);
                    }
                    verifiedPrincipal = requestPrincipal;
                    LOG.trace("Got principal {} from oauth token", entity);
                } else {
                    verifiedPrincipal = AuthCache.get(entity);
                    if (verifiedPrincipal == null) {
                        Object newObj = new Object();
                        Object o = fetchingPrincipal.getOrDefault(entity, newObj);
                        if (o == null) {
                            o = newObj;
                        }
                        try {
                            synchronized (o) {
                                // check again if in the AuthCache
                                verifiedPrincipal = AuthCache.get(entity);
                                if (verifiedPrincipal == null) {
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
                                    verifiedPrincipal = new TimelyPrincipal(authenticatedTimelyUsers);
                                    AuthCache.put(verifiedPrincipal.getName(), verifiedPrincipal);
                                    LOG.debug("Authenticated user {} for request {} with authorizations {}",
                                            verifiedPrincipal.getName(), request.toString(),
                                            verifiedPrincipal.getAuthorizationsString());
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
                }
            } catch (RuntimeException e) {
                LOG.error(e.getMessage(), e);
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "Access denied", e.getMessage());
            }
        }
        try {
            checkAllowedAccess(verifiedPrincipal.getPrimaryUser());
        } catch (TimelyException e) {
            // validation based on roles and/or auths failed
            // remove combined principal and primary user from cache
            // i.e. don't cache a failure
            AuthCache.remove(verifiedPrincipal.getName());
            AuthCache.remove(verifiedPrincipal.getPrimaryUser().getName());
            throw e;
        }

    }

    private static void checkAllowedAccess(TimelyUser primaryUser) throws TimelyException {
        Set<String> missingRoles = new TreeSet<>();
        Set<String> missingAuths = new TreeSet<>();
        if (requiredRoles != null) {
            if (!primaryUser.getRoles().containsAll(requiredRoles)) {
                missingRoles.addAll(requiredRoles);
                missingRoles.removeAll(primaryUser.getRoles());
            }
        }
        if (requiredAuths != null) {
            if (!primaryUser.getAuths().containsAll(requiredAuths)) {
                missingAuths.addAll(requiredRoles);
                missingAuths.removeAll(primaryUser.getAuths());
            }
        }

        if (!missingRoles.isEmpty() || !missingAuths.isEmpty()) {
            String message = "";
            if (!missingRoles.isEmpty() && !missingAuths.isEmpty()) {
                message = "User:" + primaryUser.getName() + " is missing role(s):" + missingRoles + " and auth(s):"
                        + missingAuths;

            } else if (!missingRoles.isEmpty()) {
                message = "User:" + primaryUser.getName() + " is missing role(s):" + missingRoles;
            } else if (missingAuths.isEmpty()) {
                message = "User:" + primaryUser.getName() + " is missing auths(s):" + missingAuths;
            }
            LOG.debug(message);
            throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "Access denied", message);
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

    public static TimelyPrincipal createPrincipalFromToken(String token) {
        return new TimelyPrincipal(JWTTokenHandler.createUsersFromToken(token, PRINCIPALS_CLAIM));
    }

    public static TimelyPrincipal createPrincipalFromHeaders(Multimap<String, String> httpHeaders) {
        String header = HttpHeaderUtils.getSingleHeader(httpHeaders, AUTH_HEADER, true);
        String token = header.substring("Bearer".length()).trim();
        return createPrincipalFromToken(token);
    }

    public static TimelyAuthenticationToken getAuthenticationToken(Multimap<String, String> headers) {
        TimelyAuthenticationToken authenticationToken = null;
        try {
            TimelyPrincipal timelyPrincipal = createPrincipalFromHeaders(headers);
            authenticationToken = new TimelyAuthenticationToken(timelyPrincipal, headers);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
        return authenticationToken;
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

    public static TimelyAuthenticationToken getAuthenticationToken(Object clientCert, String subjectDn,
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
        Object o = authentication.getPrincipal();
        Collection<String> roles = null;
        if (o instanceof TimelyUserDetails) {
            roles = ((TimelyUserDetails) o).getRoles();
        }
        TimelyUser timelyUser = new TimelyUser(subjectIssuerDNPair, userType, auths, roles, null,
                System.currentTimeMillis());
        return new TimelyPrincipal(Arrays.asList(timelyUser));
    }
}

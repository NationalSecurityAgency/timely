package timely.common.component;

import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Component;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Multimap;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.ssl.SslHandler;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.AuthenticatedWebSocketRequest;
import timely.api.response.TimelyException;
import timely.auth.JWTTokenHandler;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyAuthenticationToken;
import timely.auth.TimelyPrincipal;
import timely.auth.TimelyUser;
import timely.auth.util.AuthorizationsUtil;
import timely.auth.util.HttpHeaderUtils;
import timely.common.configuration.SecurityProperties;

@Component
public class AuthenticationService {

    private static final Logger log = LoggerFactory.getLogger(AuthenticationService.class);
    public static final String AUTH_HEADER = "Authorization";
    public static final String PRINCIPALS_CLAIM = "principals";

    private AuthenticationManager authenticationManager;
    private ConcurrentHashMap<String,Object> fetchingEntity = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String,Object> fetchingPrincipal = new ConcurrentHashMap<>();

    private SecurityProperties securityProperties;
    protected AuthCache authCache;

    public AuthenticationService(SecurityProperties securityProperties, TimelyAuthenticationManager authenticationManager) {
        this.securityProperties = securityProperties;
        this.authenticationManager = authenticationManager;
        this.authCache = new AuthCache(this, securityProperties);
    }

    public TimelyPrincipal authenticate(Authentication authentication, SubjectIssuerDNPair pair, String entity) {
        return authenticate(authentication, pair, entity, true);
    }

    public TimelyPrincipal authenticate(Authentication authentication, SubjectIssuerDNPair pair, String entity, boolean useCache) {

        // only one thread should call the authenticationManager for a given entity
        TimelyPrincipal principal = null;
        if (useCache) {
            principal = this.authCache.get(entity);
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
                        principal = this.authCache.get(entity);
                    }
                    if (principal == null) {
                        Authentication token = authenticationManager.authenticate(authentication);
                        principal = getTimelyPrincipal(token, pair);
                        log.trace("Got entity principal for {} from AuthenticationManager", entity);
                        if (useCache) {
                            this.authCache.put(entity, principal);
                        }
                    } else {
                        log.trace("Got entity principal for {} from AuthCache on 2nd attempt", entity);
                    }
                }
            } finally {
                fetchingEntity.remove(entity);
            }
        } else {
            log.trace("Got entity principal for {} from AuthCache on 1st attempt", entity);
        }
        return principal;
    }

    public void enforceAccess(AuthenticatedRequest request) throws Exception {
        String sessionId = request.getSessionId();
        X509Certificate clientCert = request.getToken().getClientCert();
        TimelyPrincipal verifiedPrincipal;
        String oauthToken = request.getRequestHeader(AUTH_HEADER);
        if (StringUtils.isBlank(sessionId) && StringUtils.isBlank(oauthToken) && clientCert == null) {
            throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must authenticate",
                            "User must authenticate with a client certificate, OAuth token, or login credentials");
        } else if (StringUtils.isNotBlank(sessionId)) {
            verifiedPrincipal = this.authCache.get(sessionId);
            if (verifiedPrincipal == null) {
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "User must authenticate", "Unknown session id was submitted, log in again");
            }
        } else {
            try {
                TimelyAuthenticationToken token = request.getToken();
                TimelyPrincipal requestPrincipal = token.getTimelyPrincipal();
                String entity = requestPrincipal.getName();
                if (oauthToken != null) {
                    // if the requestPrincipal came from an oauthToken, use that
                    TimelyPrincipal cachedPrincipal = this.authCache.get(entity);
                    if (cachedPrincipal == null || (cachedPrincipal.getCreationTime() != requestPrincipal.getCreationTime())) {
                        this.authCache.put(entity, requestPrincipal);
                    }
                    verifiedPrincipal = requestPrincipal;
                    log.trace("Got principal {} from oauth token", entity);
                } else {
                    verifiedPrincipal = this.authCache.get(entity);
                    if (verifiedPrincipal == null) {
                        Object newObj = new Object();
                        Object o = fetchingPrincipal.getOrDefault(entity, newObj);
                        if (o == null) {
                            o = newObj;
                        }
                        try {
                            synchronized (o) {
                                // check again if in the AuthCache
                                verifiedPrincipal = this.authCache.get(entity);
                                if (verifiedPrincipal == null) {
                                    // first thread here, so authenticate all entities in requestPrincipal
                                    List<TimelyUser> authenticatedTimelyUsers = new ArrayList<>();
                                    for (TimelyUser user : requestPrincipal.getProxiedUsers()) {
                                        SubjectIssuerDNPair p = user.getDn();
                                        TimelyAuthenticationToken entityToken = getAuthenticationToken(clientCert, p.subjectDN(), p.issuerDN());
                                        TimelyPrincipal entityPrincipal = authenticate(entityToken, p, entityToken.getTimelyPrincipal().getName());
                                        authenticatedTimelyUsers.addAll(entityPrincipal.getProxiedUsers());
                                    }
                                    verifiedPrincipal = new TimelyPrincipal(authenticatedTimelyUsers);
                                    this.authCache.put(verifiedPrincipal.getName(), verifiedPrincipal);
                                    log.debug("Authenticated user {} for request {} with authorizations {}", verifiedPrincipal.getName(), request.toString(),
                                                    verifiedPrincipal.getAuthorizationsString());
                                } else {
                                    log.trace("Verified principal {} in AuthCache on 2nd attempt", entity);
                                }
                            }
                        } finally {
                            fetchingPrincipal.remove(entity);
                        }
                    } else {
                        log.trace("Verified principal {} in AuthCache on 1st attempt", entity);
                    }
                }
            } catch (RuntimeException e) {
                log.error(e.getMessage(), e);
                throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "Access denied", e.getMessage());
            }
        }
        try {
            checkAllowedAccess(verifiedPrincipal.getPrimaryUser());
        } catch (TimelyException e) {
            // validation based on roles and/or auths failed
            // remove combined principal and primary user from cache
            // i.e. don't cache a failure
            this.authCache.remove(verifiedPrincipal.getName());
            this.authCache.remove(verifiedPrincipal.getPrimaryUser().getName());
            throw e;
        }

    }

    private void checkAllowedAccess(TimelyUser primaryUser) throws TimelyException {
        Set<String> missingRoles = new TreeSet<>();
        Set<String> missingAuths = new TreeSet<>();
        if (securityProperties.getRequiredRoles() != null) {
            if (!primaryUser.getRoles().containsAll(securityProperties.getRequiredRoles())) {
                missingRoles.addAll(securityProperties.getRequiredRoles());
                missingRoles.removeAll(primaryUser.getRoles());
            }
        }
        if (securityProperties.getRequiredAuths() != null) {
            if (!primaryUser.getAuths().containsAll(securityProperties.getRequiredAuths())) {
                missingAuths.addAll(securityProperties.getRequiredAuths());
                missingAuths.removeAll(primaryUser.getAuths());
            }
        }

        if (!missingRoles.isEmpty() || !missingAuths.isEmpty()) {
            String message = "";
            if (!missingRoles.isEmpty() && !missingAuths.isEmpty()) {
                message = "User:" + primaryUser.getName() + " is missing role(s):" + missingRoles + " and auth(s):" + missingAuths;

            } else if (!missingRoles.isEmpty()) {
                message = "User:" + primaryUser.getName() + " is missing role(s):" + missingRoles;
            } else if (missingAuths.isEmpty()) {
                message = "User:" + primaryUser.getName() + " is missing auths(s):" + missingAuths;
            }
            log.debug(message);
            throw new TimelyException(HttpResponseStatus.UNAUTHORIZED.code(), "Access denied", message);
        }
    }

    public X509Certificate getClientCertificate(ChannelHandlerContext ctx) {
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

    public TimelyPrincipal createPrincipalFromToken(String token) {
        return new TimelyPrincipal(JWTTokenHandler.createUsersFromToken(token, PRINCIPALS_CLAIM));
    }

    public TimelyPrincipal createPrincipalFromHeaders(Multimap<String,String> httpHeaders) {
        String header = HttpHeaderUtils.getSingleHeader(httpHeaders, AUTH_HEADER, true);
        String token = header.substring("Bearer".length()).trim();
        return createPrincipalFromToken(token);
    }

    public TimelyAuthenticationToken getAuthenticationToken(Multimap<String,String> headers) {
        TimelyAuthenticationToken authenticationToken = null;
        try {
            TimelyPrincipal timelyPrincipal = createPrincipalFromHeaders(headers);
            authenticationToken = new TimelyAuthenticationToken(timelyPrincipal, headers);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return authenticationToken;
    }

    public TimelyAuthenticationToken getAuthenticationToken(X509Certificate clientCert, Multimap<String,String> headers) {
        TimelyAuthenticationToken authenticationToken = null;
        try {
            authenticationToken = new TimelyAuthenticationToken(clientCert.getSubjectDN().getName(), clientCert, headers);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return authenticationToken;
    }

    public TimelyAuthenticationToken getAuthenticationToken(Object clientCert, String subjectDn, String issuerDn) {
        TimelyAuthenticationToken authenticationToken = null;
        try {
            authenticationToken = new TimelyAuthenticationToken(subjectDn, issuerDn, clientCert);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return authenticationToken;
    }

    private TimelyPrincipal getTimelyPrincipal(Authentication authentication, SubjectIssuerDNPair subjectIssuerDNPair) {

        Object o = authentication.getPrincipal();
        TimelyPrincipal timelyPrincipal;
        if (o instanceof TimelyPrincipal) {
            timelyPrincipal = (TimelyPrincipal) o;
        } else {
            Collection<String> auths = authentication.getAuthorities().stream().map(a -> a.getAuthority()).collect(Collectors.toList());
            TimelyUser timelyUser = new TimelyUser(subjectIssuerDNPair, TimelyUser.UserType.USER, auths, null, null, System.currentTimeMillis());
            timelyPrincipal = new TimelyPrincipal(Arrays.asList(timelyUser));
        }
        return timelyPrincipal;
    }

    private TimelyPrincipal getTimelyPrincipal(String key) {
        log.debug("Refreshing TimelyPrincipal {}", key);
        TimelyPrincipal timelyPrincipal = null;
        String[] dnArray = key.split(" -> ");
        if (dnArray.length == 1) {
            SubjectIssuerDNPair pair = SubjectIssuerDNPair.parse(dnArray[0]);
            TimelyAuthenticationToken token = getAuthenticationToken(new Object(), pair.subjectDN(), pair.issuerDN());
            timelyPrincipal = authenticate(token, pair, token.getTimelyPrincipal().getName(), false);
        } else if (dnArray.length > 1) {
            Collection<TimelyUser> timelyUsers = new ArrayList<>();
            for (String s : dnArray) {
                SubjectIssuerDNPair pair = SubjectIssuerDNPair.parse(s);
                TimelyAuthenticationToken token = getAuthenticationToken(new Object(), pair.subjectDN(), pair.issuerDN());
                TimelyPrincipal p = authenticate(token, pair, token.getTimelyPrincipal().getName(), false);
                timelyUsers.addAll(p.getProxiedUsers());
            }
            timelyPrincipal = new TimelyPrincipal(timelyUsers);
        }
        return timelyPrincipal;
    }

    public Collection<Authorizations> getAuthorizations(AuthenticatedRequest request) {
        return this.authCache.getAuthorizations(request);
    }

    public AuthCache getAuthCache() {
        return authCache;
    }

    public static class AuthCache {

        private Cache<String,TimelyPrincipal> CACHE = null;

        private int cacheExpirationMinutes;
        private int cacheRefreshMinutes;
        private AuthenticationService authenticationService;
        private SecurityProperties securityProperties;

        public AuthCache(AuthenticationService authenticationService, SecurityProperties securityProperties) {
            this.cacheExpirationMinutes = securityProperties.getCacheExpirationMinutes();
            this.cacheRefreshMinutes = securityProperties.getCacheRefreshMinutes();
            this.authenticationService = authenticationService;
            this.securityProperties = securityProperties;
        }

        private Cache<String,TimelyPrincipal> getCache() {
            if (-1 == cacheExpirationMinutes) {
                throw new IllegalStateException("Cache session max age not configured.");
            }
            if (null == CACHE) {
                Caffeine<Object,Object> caffeine = Caffeine.newBuilder();
                caffeine.expireAfterWrite(cacheExpirationMinutes, TimeUnit.MINUTES);
                if (cacheRefreshMinutes > 0) {
                    caffeine.refreshAfterWrite(cacheRefreshMinutes, TimeUnit.MINUTES);
                    CACHE = caffeine.build(key -> this.authenticationService.getTimelyPrincipal(key));
                } else {
                    CACHE = caffeine.build();
                }
            }
            return CACHE;
        }

        public boolean containsKey(String key) {
            return getCache().getIfPresent(key) != null;
        }

        public TimelyPrincipal get(String key) {
            return getCache().getIfPresent(key);
        }

        public void put(String key, TimelyPrincipal principal) {
            getCache().put(key, principal);
        }

        public void remove(String key) {
            getCache().invalidate(key);
        }

        public void clear() {
            if (CACHE != null) {
                CACHE.invalidateAll();
            }
        }

        public Collection<Authorizations> getAuthorizations(String entityName) {
            if (!StringUtils.isEmpty(entityName)) {
                List<Authorizations> authorizationsList = new ArrayList<>();
                TimelyPrincipal principal = get(entityName);
                if (principal != null) {
                    for (Collection<String> authCollection : principal.getAuthorizations()) {
                        authorizationsList.add(AuthorizationsUtil.toAuthorizations(authCollection));
                    }
                    log.debug("Authorizations for user {} {}", entityName, principal.getAuthorizationsString());
                    return authorizationsList;
                } else {
                    return Collections.singletonList(Authorizations.EMPTY);
                }
            } else {
                throw new IllegalArgumentException("entityName can not be null");
            }
        }

        public Collection<Authorizations> getAuthorizations(AuthenticatedRequest request) {
            Collection<Authorizations> auths;
            boolean anonAccessAllowed;
            if (request instanceof AuthenticatedWebSocketRequest) {
                anonAccessAllowed = securityProperties.isAllowAnonymousWsAccess();
            } else {
                anonAccessAllowed = securityProperties.isAllowAnonymousHttpAccess();
            }
            TimelyAuthenticationToken token = request.getToken();
            String sessionId = request.getSessionId();
            String oauthToken = request.getRequestHeader(AuthenticationService.AUTH_HEADER);

            if (oauthToken != null) {
                auths = getAuthorizations(token.getTimelyPrincipal().getName());
            } else if (token == null || (StringUtils.isBlank(sessionId) && token.getClientCert() == null)) {
                if (anonAccessAllowed) {
                    auths = Collections.singletonList(Authorizations.EMPTY);
                } else {
                    throw new IllegalArgumentException("User must authenticate with a client certificate, OAuth token, or login credentials");
                }
            } else if (StringUtils.isNotBlank(sessionId)) {
                auths = getAuthorizations(sessionId);
            } else {
                auths = getAuthorizations(token.getTimelyPrincipal().getName());
            }
            return auths;
        }
    }
}

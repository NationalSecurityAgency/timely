package timely.auth;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.AuthenticatedWebSocketRequest;
import timely.auth.util.AuthorizationsUtil;
import timely.configuration.Security;
import timely.netty.http.auth.TimelyAuthenticationToken;

public class AuthCache {

    private static final Logger LOG = LoggerFactory.getLogger(AuthCache.class);
    private static Cache<String, TimelyPrincipal> CACHE = null;

    private static int cacheExpirationMinutes = -1;
    private static int cacheRefreshMinutes = -1;

    /**
     * For tests only
     */
    public static void resetConfiguration() {
        cacheExpirationMinutes = -1;
        cacheRefreshMinutes = -1;
    }

    public static void configure(Security security) {
        if (-1 != cacheExpirationMinutes || -1 != cacheRefreshMinutes) {
            throw new IllegalStateException("Cache already configured.");
        }
        cacheExpirationMinutes = security.getCacheExpirationMinutes();
        cacheRefreshMinutes = security.getCacheRefreshMinutes();
    }

    private static Cache<String, TimelyPrincipal> getCache() {
        if (-1 == cacheExpirationMinutes) {
            throw new IllegalStateException("Cache session max age not configured.");
        }
        if (null == CACHE) {
            Caffeine<Object, Object> caffeine = Caffeine.newBuilder();
            caffeine.expireAfterWrite(cacheExpirationMinutes, TimeUnit.MINUTES);
            if (cacheRefreshMinutes > 0) {
                caffeine.refreshAfterWrite(cacheRefreshMinutes, TimeUnit.MINUTES);
                CACHE = caffeine.build(key -> getTimelyPrincipal(key));
            } else {
                CACHE = caffeine.build();
            }
        }
        return CACHE;
    }

    private static TimelyPrincipal getTimelyPrincipal(String key) {
        LOG.debug("Refreshing TimelyPrincipal {}", key);
        TimelyPrincipal timelyPrincipal = null;
        String[] dnArray = key.split(" -> ");
        if (dnArray.length == 1) {
            SubjectIssuerDNPair pair = SubjectIssuerDNPair.parse(dnArray[0]);
            TimelyAuthenticationToken token = AuthenticationService.getAuthenticationToken(new Object(),
                    pair.subjectDN(), pair.issuerDN());
            timelyPrincipal = AuthenticationService.authenticate(token, pair, token.getTimelyPrincipal().getName(),
                    false);
        } else if (dnArray.length > 1) {
            Collection<TimelyUser> timelyUsers = new ArrayList<>();
            for (String s : dnArray) {
                SubjectIssuerDNPair pair = SubjectIssuerDNPair.parse(s);
                TimelyAuthenticationToken token = AuthenticationService.getAuthenticationToken(new Object(),
                        pair.subjectDN(), pair.issuerDN());
                TimelyPrincipal p = AuthenticationService.authenticate(token, pair,
                        token.getTimelyPrincipal().getName(), false);
                timelyUsers.addAll(p.getProxiedUsers());
            }
            timelyPrincipal = new TimelyPrincipal(timelyUsers);
        }
        return timelyPrincipal;
    }

    public static boolean containsKey(String key) {
        return getCache().getIfPresent(key) != null;
    }

    public static TimelyPrincipal get(String key) {
        return getCache().getIfPresent(key);
    }

    public static void put(String key, TimelyPrincipal principal) {
        getCache().put(key, principal);
    }

    public static void remove(String key) {
        getCache().invalidate(key);
    }

    public static void clear() {
        if (CACHE != null) {
            CACHE.invalidateAll();
        }
    }

    protected static Collection<Authorizations> getAuthorizations(String entityName) {
        if (!StringUtils.isEmpty(entityName)) {
            List<Authorizations> authorizationsList = new ArrayList<>();
            TimelyPrincipal principal = AuthCache.get(entityName);
            if (principal != null) {
                for (Collection<String> authCollection : principal.getAuthorizations()) {
                    authorizationsList.add(AuthorizationsUtil.toAuthorizations(authCollection));
                }
                LOG.debug("Authorizations for user {} {}", entityName, principal.getAuthorizationsString());
                return authorizationsList;
            } else {
                return Collections.singletonList(Authorizations.EMPTY);
            }
        } else {
            throw new IllegalArgumentException("entityName can not be null");
        }
    }

    public static Collection<Authorizations> getAuthorizations(AuthenticatedRequest request, Security security) {
        Collection<Authorizations> auths;
        boolean anonAccessAllowed;
        if (request instanceof AuthenticatedWebSocketRequest) {
            anonAccessAllowed = security.isAllowAnonymousWsAccess();
        } else {
            anonAccessAllowed = security.isAllowAnonymousHttpAccess();
        }
        TimelyAuthenticationToken token = request.getToken();
        String sessionId = request.getSessionId();
        if (StringUtils.isBlank(sessionId) && token.getClientCert() == null) {
            if (anonAccessAllowed) {
                auths = Collections.singletonList(Authorizations.EMPTY);
            } else {
                throw new IllegalArgumentException("User must provide either a sessionId or a client certificate");
            }
        } else if (StringUtils.isNotBlank(sessionId)) {
            auths = getAuthorizations(sessionId);
        } else {
            auths = getAuthorizations(token.getTimelyPrincipal().getName());
        }
        return auths;
    }
}

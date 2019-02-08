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

    private static int sessionMaxAge = -1;

    /**
     * For tests only
     */
    public static void resetSessionMaxAge() {
        sessionMaxAge = -1;
    }

    public static void setSessionMaxAge(Security security) {
        if (-1 != sessionMaxAge) {
            throw new IllegalStateException("Cache session max age already configured.");
        }
        sessionMaxAge = security.getSessionMaxAge();
    }

    private static Cache<String, TimelyPrincipal> getCache() {
        if (-1 == sessionMaxAge) {
            throw new IllegalStateException("Cache session max age not configured.");
        }
        if (null == CACHE) {
            CACHE = Caffeine.newBuilder().expireAfterAccess(sessionMaxAge, TimeUnit.SECONDS).build();
        }
        return CACHE;
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

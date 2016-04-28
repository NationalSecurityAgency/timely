package timely.auth;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;

import java.util.concurrent.TimeUnit;

public class VisibilityCache {

    private static volatile Cache<String, ColumnVisibility> CACHE = null;

    private static boolean warned = false;

    /** Used for testing */
    public static synchronized void init() {
        if (CACHE == null) {
            long defaultExpiration = Configuration.VISIBILITY_EXPIRATION_DEFAULT;
            int initialCapacity = Configuration.VISIBILITY_CACHE_INITIAL_CAPACITY_DEFAULT;
            long maxCapacity = Configuration.VISIBILITY_CACHE_MAX_CAPACITY_DEFAULT;
            CACHE = Caffeine.newBuilder().expireAfterAccess(defaultExpiration, TimeUnit.MINUTES)
                    .initialCapacity(initialCapacity).maximumSize(maxCapacity).build();
        }
    }

    public static synchronized void init(Configuration config) {
        if (CACHE == null) {
            long expireMinutes = Long.parseLong(config.get(Configuration.VISIBILITY_CACHE_EXPIRATION));
            int initialCapacity = Integer.parseInt(config.get(Configuration.VISIBILITY_CACHE_INITIAL_CAPACITY));
            long maxCapacity = Long.parseLong(config.get(Configuration.VISIBILITY_CACHE_MAX_CAPACITY));
            CACHE = Caffeine.newBuilder().expireAfterAccess(expireMinutes, TimeUnit.MINUTES)
                    .initialCapacity(initialCapacity).maximumSize(maxCapacity).build();
        }
    }

    public static ColumnVisibility getColumnVisibility(String visibilityString) {
        return CACHE.get(visibilityString, key -> generateNormalizedVisibility(key));
    }

    private static final ColumnVisibility generateNormalizedVisibility(String visibilityString) {
        // it would be nice if Accumulo would make it easier to do this.
        return new ColumnVisibility(new ColumnVisibility(visibilityString).flatten());
    }
}

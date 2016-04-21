package timely.store;

import timely.Configuration;

public class MetaCacheFactory {

    private static MetaCache cache = null;

    public static MetaCache getCache() {
        if (null != cache) {
            return cache;
        } else {
            throw new RuntimeException("MetaCache not initialized.");
        }
    }

    public static final synchronized MetaCache getCache(Configuration conf) {
        if (null == cache || cache.isClosed()) {
            if (null == conf) {
                throw new RuntimeException("Configuration cannot be null");
            }
            cache = new MetaCacheImpl();
            cache.init(conf);
        }
        return cache;
    }

    public static final synchronized void close() {
        if (null != cache) {
            cache.close();
        }
        cache = null;
    }

}

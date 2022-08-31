package timely.server.test;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.common.component.AuthenticationService;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.TimelyProperties;
import timely.server.store.InternalMetrics;
import timely.server.store.cache.DataStoreCache;

public class TestDataStoreCache extends DataStoreCache {

    private static final Logger log = LoggerFactory.getLogger(DataStoreCache.class);

    public TestDataStoreCache(CuratorFramework curatorFramework, AuthenticationService authenticationService, InternalMetrics internalMetrics,
                    TimelyProperties timelyProperties, CacheProperties cacheProperties) {
        super(curatorFramework, authenticationService, internalMetrics, timelyProperties, cacheProperties);
    }

    @Override
    protected void testIPRWLock(CuratorFramework curatorFramework, InterProcessReadWriteLock lock, String path) {

    }

    @Override
    protected void addNonCachedMetricsListener(CuratorFramework curatorFramework) {

    }
}

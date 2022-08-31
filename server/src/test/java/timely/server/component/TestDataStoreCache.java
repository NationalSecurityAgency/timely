package timely.server.component;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import timely.common.component.AuthenticationService;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.TimelyProperties;
import timely.store.InternalMetrics;

@Primary
@Component
public class TestDataStoreCache extends DataStoreCache {

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

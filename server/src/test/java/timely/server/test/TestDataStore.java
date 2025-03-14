package timely.server.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import timely.common.component.AuthenticationService;
import timely.common.configuration.AccumuloProperties;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.TimelyProperties;
import timely.common.configuration.ZookeeperProperties;
import timely.model.Metric;
import timely.server.store.DataStore;
import timely.server.store.InternalMetrics;
import timely.server.store.MetaCache;
import timely.server.store.cache.DataStoreCache;
import timely.util.Exclusions;

public class TestDataStore extends DataStore {

    private static final Logger log = LoggerFactory.getLogger(TestDataStore.class);
    private final MetaCache metaCache;
    private List<StoreCallback> storeCallbacks = Collections.synchronizedList(new ArrayList<>());

    public TestDataStore(ApplicationContext applicationContext, AccumuloClient accumuloClient, DataStoreCache dataStoreCache,
                    AuthenticationService authenticationService, InternalMetrics internalMetrics, MetaCache metaCache, TimelyProperties timelyProperties,
                    ZookeeperProperties zookeeperProperties, AccumuloProperties accumuloProperties, SecurityProperties securityProperties,
                    CacheProperties cacheProperties, Exclusions exclusions) {
        super(applicationContext, accumuloClient, dataStoreCache, authenticationService, internalMetrics, metaCache, timelyProperties, zookeeperProperties,
                        accumuloProperties, securityProperties, cacheProperties, exclusions);
        this.metaCache = metaCache;
    }

    public void reset() {
        storeCallbacks.clear();
        dataStoreResetLock.writeLock().lock();
        try {
            synchronized (writers) {
                writers.forEach(w -> {
                    try {
                        w.close();
                    } catch (MutationsRejectedException e) {
                        log.error(e.getMessage(), e);
                    }
                });
                writers.clear();
            }
            dataStoreCache.clear();
            metaCache.clear();
            authenticationService.getAuthCache().clear();
            configureAgeOff(timelyProperties);
            applyAgeOffIterators();
        } finally {
            dataStoreResetLock.writeLock().unlock();
        }
    }

    @Override
    public void store(Metric metric, boolean cacheEnabled) {
        super.store(metric, cacheEnabled);
        synchronized (storeCallbacks) {
            for (StoreCallback c : storeCallbacks) {
                c.onStore();
            }
        }
    }

    public void addStoreCallback(StoreCallback storeCallback) {
        storeCallbacks.add(storeCallback);
    }

    public void removeStoreCallback(StoreCallback storeCallback) {
        storeCallbacks.remove(storeCallback);
    }

    public interface StoreCallback {
        public void onStore();
    }
}

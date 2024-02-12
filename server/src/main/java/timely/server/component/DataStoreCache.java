package timely.server.component;

import static timely.Constants.NON_CACHED_METRICS;
import static timely.Constants.NON_CACHED_METRICS_LOCK_PATH;
import static timely.common.configuration.CacheProperties.DEFAULT_AGEOFF_KEY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.StampedLock;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicValue;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.stereotype.Component;

import io.netty.handler.codec.http.HttpResponseStatus;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.CacheResponse;
import timely.api.response.MetricResponse;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.auth.util.AuthorizationsUtil;
import timely.common.component.AuthenticationService;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.TimelyProperties;
import timely.model.Metric;
import timely.model.Tag;
import timely.sample.Aggregation;
import timely.sample.Aggregator;
import timely.sample.Sample;
import timely.sample.iterators.AggregationIterator;
import timely.sample.iterators.DownsampleIterator;
import timely.store.InternalMetrics;
import timely.store.MetricAgeOffIterator;
import timely.store.cache.DataStoreCacheIterator;
import timely.store.cache.GorillaStore;
import timely.store.cache.TaggedMetric;
import timely.store.cache.VisibilityFilter;
import timely.store.iterators.RateIterator;

@Component
@ConditionalOnMissingBean(DataStoreCache.class)
public class DataStoreCache {

    private static final Logger log = LoggerFactory.getLogger(DataStoreCache.class);

    private Map<String,Map<TaggedMetric,GorillaStore>> gorillaMap = new HashMap<>();
    private StampedLock gorillaMapLock = new StampedLock();

    private Set<String> nonCachedMetrics = Collections.synchronizedSet(new HashSet<>());
    private DistributedAtomicValue nonCachedMetricsIP = null;
    private InterProcessReadWriteLock nonCachedMetricsIPRWLock;
    private long staleCacheExpiration;
    private long maxUniqueTagSets;
    private Map<String,Long> minimumAgeOff;
    private Map<String,String> minimumAgeOffForIterator;
    private int flushBatch = 0;
    private int numBatches = 5;
    private InternalMetrics internalMetrics;
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(6);
    private AuthenticationService authenticationService;
    private CuratorFramework curatorFramework;
    private TimelyProperties timelyProperties;
    private CacheProperties cacheProperties;

    public DataStoreCache(CuratorFramework curatorFramework, AuthenticationService authenticationService, InternalMetrics internalMetrics,
                    TimelyProperties timelyProperties, CacheProperties cacheProperties) {
        this.curatorFramework = curatorFramework;
        this.authenticationService = authenticationService;
        this.internalMetrics = internalMetrics;
        this.timelyProperties = timelyProperties;
        this.cacheProperties = cacheProperties;
        this.maxUniqueTagSets = cacheProperties.getMaxUniqueTagSets();
        this.staleCacheExpiration = cacheProperties.getStaleCacheExpiration();
    }

    @PostConstruct
    public void setup() throws Exception {
        if (curatorFramework != null) {
            addNonCachedMetricsListener(curatorFramework);
        }
        log.info("Adding initial values from configuration");
        addNonCachedMetrics(cacheProperties.getNonCachedMetrics());
        log.info("Reading initial values from nonCachedMetricsIP");
        readNonCachedMetricsIP();
        configureAgeOff(timelyProperties, cacheProperties);

        executorService.scheduleAtFixedRate(() -> {
            try {
                flushCaches(flushBatch);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                flushBatch++;
                if (flushBatch == numBatches) {
                    flushBatch = 0;
                }
            }
        }, cacheProperties.getFlushInterval(), cacheProperties.getFlushInterval(), TimeUnit.MILLISECONDS);

        executorService.scheduleAtFixedRate(() -> {
            try {
                DataStoreCache.this.removeStaleMetrics();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 5, 5, TimeUnit.MINUTES);

        executorService.scheduleAtFixedRate(() -> {
            try {
                ageOffGorillaStores();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 10, 10, TimeUnit.MINUTES);

        executorService.scheduleAtFixedRate(() -> {
            try {
                archiveGorillaStoreCurrentCompressors();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 60, 60, TimeUnit.MINUTES);

        executorService.scheduleAtFixedRate(() -> {
            try {
                pruneStats();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }, 10, 10, TimeUnit.MINUTES);

        if (!timelyProperties.getTest()) {
            executorService.scheduleAtFixedRate(() -> {
                try {
                    reportInternalMetrics();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }, 1, 1, TimeUnit.MINUTES);
        }
    }

    @PreDestroy
    public void shutdown() {
        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        } finally {
            if (!this.executorService.isTerminated()) {
                this.executorService.shutdownNow();
            }
        }
    }

    public void configureAgeOff(TimelyProperties timelyProperties, CacheProperties cacheProperties) {
        Map<String,Integer> cacheAgeOff = cacheProperties.getMetricAgeOffHours();
        Map<String,Integer> accumuloAgeOff = timelyProperties.getMetricAgeOffDays();
        log.info("cacheAgeOff:{} accumuloAgeOff:{}", cacheAgeOff, accumuloAgeOff);
        minimumAgeOff = getMinimumAgeOffs(accumuloAgeOff, cacheAgeOff);

        for (Map.Entry<String,Long> e : minimumAgeOff.entrySet()) {
            log.info("minimumAgeOff key:{} value:{}", e.getKey(), e.getValue());
        }

        minimumAgeOffForIterator = getAgeOffForIterator(minimumAgeOff);
    }

    private void addNonCachedMetrics(Collection<String> nonCachedMetricsUpdate) {
        if (!nonCachedMetricsUpdate.isEmpty()) {
            try {
                log.info("Adding {} to local nonCachedMetrics", nonCachedMetricsUpdate);
                nonCachedMetrics.addAll(nonCachedMetricsUpdate);
                if (nonCachedMetricsIP != null) {
                    try {
                        nonCachedMetricsIPRWLock.writeLock().acquire();
                        byte[] currentNonCachedMetricsDistributedBytes = nonCachedMetricsIP.get().postValue();
                        Set<String> currentNonCachedMetricsIP;
                        if (currentNonCachedMetricsDistributedBytes == null) {
                            currentNonCachedMetricsIP = new TreeSet<>();
                        } else {
                            try {
                                currentNonCachedMetricsIP = SerializationUtils.deserialize(currentNonCachedMetricsDistributedBytes);
                            } catch (Exception e) {
                                log.error(e.getMessage());
                                currentNonCachedMetricsIP = new TreeSet<>();
                            }
                        }
                        if (currentNonCachedMetricsIP.containsAll(nonCachedMetricsUpdate)) {
                            log.info("nonCachedMetricsIP already contains {}", nonCachedMetricsUpdate);
                        } else {
                            nonCachedMetricsUpdate.removeAll(currentNonCachedMetricsIP);
                            log.info("Adding {} to nonCachedMetricsIP", nonCachedMetricsUpdate);
                            TreeSet<String> updateSet = new TreeSet<>();
                            updateSet.addAll(currentNonCachedMetricsIP);
                            updateSet.addAll(nonCachedMetricsUpdate);
                            byte[] updateValue = SerializationUtils.serialize(updateSet);
                            nonCachedMetricsIP.trySet(updateValue);
                            if (!nonCachedMetricsIP.get().succeeded()) {
                                nonCachedMetricsIP.forceSet(updateValue);
                            }
                        }
                    } finally {
                        nonCachedMetricsIPRWLock.writeLock().release();
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    protected void addNonCachedMetricsListener(CuratorFramework curatorFramework) {
        nonCachedMetricsIPRWLock = new InterProcessReadWriteLock(curatorFramework, NON_CACHED_METRICS_LOCK_PATH);
        testIPRWLock(curatorFramework, nonCachedMetricsIPRWLock, NON_CACHED_METRICS_LOCK_PATH);
        nonCachedMetricsIP = new DistributedAtomicValue(curatorFramework, NON_CACHED_METRICS, new RetryForever(1000));
        try {
            nonCachedMetricsIP.forceSet(SerializationUtils.serialize(new TreeSet<>()));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        TreeCacheListener nonCachedMetricsListener = (cf, event) -> {
            if (event.getType().equals(TreeCacheEvent.Type.NODE_UPDATED)) {
                log.info("Handling nonCachedMetricsIP event {}", event.getType().toString());
                readNonCachedMetricsIP();
            }
        };

        try (TreeCache nonCachedMetricsTreeCache = new TreeCache(curatorFramework, NON_CACHED_METRICS)) {
            nonCachedMetricsTreeCache.getListenable().addListener(nonCachedMetricsListener);
            nonCachedMetricsTreeCache.start();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void readNonCachedMetricsIP() {
        if (nonCachedMetricsIPRWLock != null) {
            try {
                nonCachedMetricsIPRWLock.readLock().acquire();
                byte[] nonCachedMetricsUpdateBytes = nonCachedMetricsIP.get().postValue();
                Set<String> nonCachedMetricsUpdate;
                if (nonCachedMetricsUpdateBytes == null) {
                    nonCachedMetricsUpdate = new TreeSet<>();
                } else {
                    nonCachedMetricsUpdate = SerializationUtils.deserialize(nonCachedMetricsIP.get().postValue());
                }

                if (nonCachedMetrics.containsAll(nonCachedMetricsUpdate)) {
                    log.info("local nonCachedMetrics already contains {}", nonCachedMetricsUpdate);
                } else {
                    nonCachedMetricsUpdate.removeAll(nonCachedMetrics);
                    log.info("Adding {} to local nonCachedMetrics", nonCachedMetricsUpdate);
                    nonCachedMetrics.addAll(nonCachedMetricsUpdate);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                try {
                    nonCachedMetricsIPRWLock.readLock().release();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }
    }

    protected void testIPRWLock(CuratorFramework curatorFramework, InterProcessReadWriteLock lock, String path) {
        try {
            lock.writeLock().acquire(10, TimeUnit.SECONDS);
        } catch (Exception e1) {
            try {
                curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
                curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);
            } catch (Exception e2) {
                log.info(e2.getMessage(), e2);
            }
        } finally {
            try {
                lock.writeLock().release();
            } catch (Exception e3) {
                log.error(e3.getMessage());
            }
        }
    }

    private void reportInternalMetrics() {
        if (internalMetrics != null) {
            long stamp = gorillaMapLock.readLock();
            try {
                long now = System.currentTimeMillis();
                long cachedEntries = 0;
                long oldestCacheEntry = Long.MAX_VALUE;
                String oldestMetric = "";
                for (Map.Entry<String,Map<TaggedMetric,GorillaStore>> e : gorillaMap.entrySet()) {
                    String metric = e.getKey();
                    Map<TaggedMetric,GorillaStore> e1 = e.getValue();
                    for (Map.Entry<TaggedMetric,GorillaStore> e2 : e1.entrySet()) {
                        cachedEntries += e2.getValue().getNumEntries();
                        long oldestTimestamp = e2.getValue().getOldestTimestamp();
                        if (oldestTimestamp < oldestCacheEntry) {
                            if (log.isTraceEnabled()) {
                                String oldestCacheEntryAge;
                                if (oldestCacheEntry == Long.MAX_VALUE) {
                                    oldestCacheEntryAge = "Long.MAX_VALUE";
                                } else {
                                    oldestCacheEntryAge = Long.toString((now - oldestCacheEntry) / (60 * 1000));
                                }
                                log.trace("setInternalMetrics changing {} oldestCacheEntry from ageMin:{} to ageMin:{}", metric, oldestCacheEntryAge,
                                                (now - oldestTimestamp) / (60 * 1000));
                            }
                            oldestCacheEntry = oldestTimestamp;
                            oldestMetric = metric;
                        }
                    }
                }
                internalMetrics.setNumCachedMetricsTotal(cachedEntries);
                long oldestCachedMetricAge;
                if (oldestCacheEntry == Long.MAX_VALUE) {
                    oldestCachedMetricAge = 0;
                } else {
                    oldestCachedMetricAge = System.currentTimeMillis() - oldestCacheEntry;
                }
                internalMetrics.setAgeOfOldestCachedMetric(oldestCachedMetricAge);
                log.trace("reporting oldest cached metric as metric:{} ageMin:{}", oldestMetric, oldestCachedMetricAge / (60 * 1000));
            } finally {
                gorillaMapLock.unlockRead(stamp);
            }
        }
    }

    private void pruneStats() {
        long stamp = gorillaMapLock.writeLock();
        try {
            Iterator<Map.Entry<String,Map<TaggedMetric,GorillaStore>>> metricItr = gorillaMap.entrySet().iterator();

            while (metricItr.hasNext()) {
                Map.Entry<String,Map<TaggedMetric,GorillaStore>> entry1 = metricItr.next();
                int numberTagVariations = entry1.getValue().size();
                if (numberTagVariations > maxUniqueTagSets) {
                    log.info("Cache of metric {} has {} tag variations.  Discontinuing cache.", entry1.getKey(), numberTagVariations);
                    metricItr.remove();
                    addNonCachedMetrics(Collections.singleton(entry1.getKey()));
                }
            }
        } finally {
            gorillaMapLock.unlockWrite(stamp);
        }
    }

    private void ageOffGorillaStores() {
        long stamp = gorillaMapLock.writeLock();
        try {
            long numRemovedTotal = 0;
            long now = System.currentTimeMillis();
            Iterator<Map.Entry<String,Map<TaggedMetric,GorillaStore>>> metricIterator = gorillaMap.entrySet().iterator();
            while (metricIterator.hasNext()) {
                Map.Entry<String,Map<TaggedMetric,GorillaStore>> metricEntry = metricIterator.next();
                String metric = metricEntry.getKey();
                long maxAge = getAgeOffForMetric(metric);
                long oldestTs = Long.MAX_VALUE;
                long numRemovedMetric = 0;
                Iterator<Map.Entry<TaggedMetric,GorillaStore>> taggedMetricItr = metricEntry.getValue().entrySet().iterator();
                while (taggedMetricItr.hasNext()) {
                    Map.Entry<TaggedMetric,GorillaStore> taggedMetricEntry = taggedMetricItr.next();
                    GorillaStore store = taggedMetricEntry.getValue();
                    numRemovedMetric += store.ageOffArchivedCompressors();
                    long oldestTimestampInStore = store.getOldestTimestamp();
                    if (oldestTimestampInStore < oldestTs) {
                        oldestTs = oldestTimestampInStore;
                    }
                    if (oldestTimestampInStore == Long.MAX_VALUE && store.isEmpty()) {
                        log.trace("Gorilla store for {}:{} empty, removing", metric, taggedMetricEntry.getKey().getTags());
                        taggedMetricItr.remove();
                    }
                }
                numRemovedTotal += numRemovedMetric;
                if (log.isTraceEnabled()) {
                    String oldestAgeMin;
                    if (oldestTs == Long.MAX_VALUE) {
                        oldestAgeMin = "Long.MAX_VALUE";
                    } else {
                        oldestAgeMin = Long.toString((now - oldestTs) / (60 * 1000));
                    }
                    log.trace("ageOffGorillaStores metric:{} with maxAgeMin:{}, oldestAgeMin:{} after aging off {} archived Gorilla compressors", metric,
                                    (maxAge / (60 * 1000)), oldestAgeMin, numRemovedTotal);
                }
            }
            log.trace("ageOffGorillaStores aged off {} archived Gorilla compressors", numRemovedTotal);
        } finally {
            gorillaMapLock.unlockWrite(stamp);
        }
    }

    public void flushCaches(int flushBatch) {
        long stamp = gorillaMapLock.readLock();
        try {
            int x = 0;
            for (Map.Entry<String,Map<TaggedMetric,GorillaStore>> entry : gorillaMap.entrySet()) {
                if (flushBatch == -1 || x % numBatches == flushBatch) {
                    for (GorillaStore store : entry.getValue().values()) {
                        try {
                            store.flush();
                        } catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }
                x++;
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
    }

    private void archiveGorillaStoreCurrentCompressors() {
        long stamp = gorillaMapLock.readLock();
        try {
            for (Map.Entry<String,Map<TaggedMetric,GorillaStore>> entry : gorillaMap.entrySet()) {
                for (GorillaStore store : entry.getValue().values()) {
                    store.archiveCurrentCompressor();
                }
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
    }

    private void removeStaleMetrics() {

        Set<String> metricsToRemove = new HashSet<>();
        long stamp = gorillaMapLock.readLock();
        try {
            long now = System.currentTimeMillis();
            for (Map.Entry<String,Map<TaggedMetric,GorillaStore>> entry : gorillaMap.entrySet()) {
                String metricName = entry.getKey();
                long totalMsecToMetric = 0;
                long numStoresWithMultipleEntries = 0;
                int tagVariations = entry.getValue().values().size();
                for (GorillaStore store : entry.getValue().values()) {
                    long numEntries = store.getNumEntries();
                    long oldestTimestamp = store.getOldestTimestamp();
                    long newestTimestamp = store.getNewestTimestamp();
                    if (numEntries > 1) {
                        long msecToMetric = (newestTimestamp - oldestTimestamp) / (numEntries - 1);
                        totalMsecToMetric += msecToMetric;
                        numStoresWithMultipleEntries++;
                    }
                }
                if (numStoresWithMultipleEntries > 0) {
                    long avgMsecToMetric = totalMsecToMetric / numStoresWithMultipleEntries;
                    long ageOfNewest = now - getNewestTimestamp(metricName, stamp);
                    // do not delete slow-arriving metrics based on this criteria
                    // only consider metrics that should have arrived in staleCacheExpiration * 0.5
                    log.trace("metric:{} tagVariations:{} avgMsecToMetric:{}", metricName, tagVariations, avgMsecToMetric);
                    if (avgMsecToMetric < (staleCacheExpiration * 0.5)) {
                        if (ageOfNewest > staleCacheExpiration) {
                            log.info("Removing metric:{} tagVariations:{} avgMsecToMetric:{} ageOfNewestEntry:{} > staleCacheExpiration:{}", metricName,
                                            tagVariations, avgMsecToMetric, ageOfNewest, staleCacheExpiration);
                            metricsToRemove.add(metricName);
                        }
                    } else {
                        log.trace("Skipping staleness evaluation of metric:{} tagVariations:{} avgMsecToMetric:{} < (0.5 * staleCacheExpiration):{}",
                                        metricName, tagVariations, avgMsecToMetric, (staleCacheExpiration / 2));
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }

        if (metricsToRemove.size() > 0) {
            stamp = gorillaMapLock.writeLock();
            try {
                for (String metricName : metricsToRemove) {
                    gorillaMap.remove(metricName);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            } finally {
                gorillaMapLock.unlockWrite(stamp);
            }
        }
    }

    public long getAgeOffForMetric(String metricName) {
        if (this.minimumAgeOff.containsKey(metricName)) {
            return this.minimumAgeOff.get(metricName);
        } else {
            return this.minimumAgeOff.get(DEFAULT_AGEOFF_KEY);
        }
    }

    public void setDefaultAgeOffMilliSec(long defaultAgeOffMilliSec) {
        this.minimumAgeOff.put(DEFAULT_AGEOFF_KEY, defaultAgeOffMilliSec);
        this.minimumAgeOffForIterator.put(MetricAgeOffIterator.AGE_OFF_PREFIX + MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, Long.toString(defaultAgeOffMilliSec));
    }

    private Map<String,Long> getMinimumAgeOffs(Map<String,Integer> accumuloAgeOffDays, Map<String,Integer> cacheAgeOffHours) {
        Map<String,Long> minimumAgeOffs = new HashMap<>();
        Set<String> keys = new HashSet<>();
        keys.addAll(accumuloAgeOffDays.keySet());
        keys.addAll(cacheAgeOffHours.keySet());
        for (String name : keys) {
            Long accumuloAgeOffValue = (accumuloAgeOffDays.containsKey(name)) ? accumuloAgeOffDays.get(name) * 86400000L : Long.MAX_VALUE;
            Long cacheAgeOffValue = (cacheAgeOffHours.containsKey(name)) ? cacheAgeOffHours.get(name) * 3600000L : Long.MAX_VALUE;
            minimumAgeOffs.put(name, Math.min(accumuloAgeOffValue, cacheAgeOffValue));
        }
        return minimumAgeOffs;
    }

    private Map<String,String> getAgeOffForIterator(Map<String,Long> minimumAgeOff) {
        Map<String,String> ageOffOptions = new HashMap<>();
        minimumAgeOff.forEach((k, v) -> {
            String ageoff = Long.toString(v);
            log.trace("Adding age off for metric: {} of {} milliseconds", k, v);
            ageOffOptions.put(MetricAgeOffIterator.AGE_OFF_PREFIX + k, ageoff);
        });
        ageOffOptions.put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, Long.toString(minimumAgeOff.get(DEFAULT_AGEOFF_KEY)));
        return ageOffOptions;
    }

    public Map<TaggedMetric,GorillaStore> getGorillaStores(String metric) {
        Map<TaggedMetric,GorillaStore> returnedMap = new HashMap<>();
        Map<TaggedMetric,GorillaStore> metricMap;
        long stamp = gorillaMapLock.readLock();
        try {
            metricMap = gorillaMap.get(metric);
            if (metricMap == null) {
                metricMap = new HashMap<>();
                long writeStamp = gorillaMapLock.tryConvertToWriteLock(stamp);
                if (writeStamp == 0) {
                    gorillaMapLock.unlockRead(stamp);
                    stamp = gorillaMapLock.writeLock();
                } else {
                    stamp = writeStamp;
                }
                gorillaMap.put(metric, metricMap);
            }
            returnedMap.putAll(metricMap);
        } finally {
            gorillaMapLock.unlock(stamp);
        }
        return returnedMap;
    }

    public GorillaStore getGorillaStore(String metric, TaggedMetric taggedMetric) {

        Map<TaggedMetric,GorillaStore> metricMap;
        GorillaStore gStore;
        boolean needWrite = false;
        long stamp = gorillaMapLock.readLock();
        try {
            metricMap = gorillaMap.get(metric);
            if (metricMap == null) {
                log.info("Creating new cache for metric:{}", metric);
                metricMap = new HashMap<>();
                needWrite = true;
            }
            gStore = metricMap.get(taggedMetric);
            if (gStore == null) {
                gStore = new GorillaStore(metric, getAgeOffForMetric(metric));
                needWrite = true;
            }
            if (needWrite) {
                long writeStamp = gorillaMapLock.tryConvertToWriteLock(stamp);
                if (writeStamp == 0) {
                    gorillaMapLock.unlockRead(stamp);
                    stamp = gorillaMapLock.writeLock();
                } else {
                    stamp = writeStamp;
                }
                metricMap.put(taggedMetric, gStore);
                gorillaMap.put(metric, metricMap);
            }
        } finally {
            gorillaMapLock.unlock(stamp);
        }
        return gStore;
    }

    private boolean shouldCache(Metric metric) {
        String metricName = metric.getName();
        if (nonCachedMetrics.contains(metricName)) {
            return false;
        }

        long stamp = gorillaMapLock.readLock();
        try {
            if (gorillaMap.containsKey(metricName)) {
                return true;
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }

        Set<String> tempNonCachedMetrics = new LinkedHashSet<>();
        synchronized (nonCachedMetrics) {
            tempNonCachedMetrics.addAll(nonCachedMetrics);
        }
        for (String r : tempNonCachedMetrics) {
            if (metricName.matches(r)) {
                addNonCachedMetrics(Collections.singleton(metricName));
                return false;
            }
        }
        return true;
    }

    public void store(Metric metric) {
        if (shouldCache(metric)) {
            if (internalMetrics != null) {
                internalMetrics.incrementMetricsCached(1);
            }
            TaggedMetric taggedMetric = new TaggedMetric(metric.getTags());
            GorillaStore gs = getGorillaStore(metric.getName(), taggedMetric);
            gs.addValue(metric);
        }
    }

    public List<QueryResponse> query(QueryRequest msg) throws TimelyException {

        List<QueryResponse> result = new ArrayList<>();
        try {
            Collection<QueryRequest.SubQuery> subQueries = msg.getQueries();
            for (QueryRequest.SubQuery query : subQueries) {
                Map<Set<Tag>,List<Aggregation>> aggregations = subquery(msg, query);
                for (Map.Entry<Set<Tag>,List<Aggregation>> entry : aggregations.entrySet()) {
                    long tsDivisor = msg.isMsResolution() ? 1 : 1000;
                    result.add(convertToQueryResponse(query, entry.getKey(), entry.getValue(), tsDivisor));
                }
            }
            return result;
        } catch (Exception e) {
            log.error("Error during query: " + e.getMessage(), e);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: " + e.getMessage(), e.getMessage(), e);
        }
    }

    public Map<Set<Tag>,List<Aggregation>> subquery(QueryRequest msg, QueryRequest.SubQuery query) throws TimelyException {

        Map<Set<Tag>,List<Aggregation>> aggregationList = new HashMap<>();
        long start = System.currentTimeMillis();
        try {
            SortedKeyValueIterator<org.apache.accumulo.core.data.Key,org.apache.accumulo.core.data.Value> itr = null;
            Collection<Authorizations> authorizations = getSessionAuthorizations(msg);
            itr = setupIterator(msg, query, authorizations, getAgeOffForMetric(query.getMetric()));
            Map<Set<Tag>,Set<Tag>> matchingTagCache = new HashMap<>();
            while (itr.hasTop()) {
                Map<Set<Tag>,Aggregation> samples = AggregationIterator.decodeValue(itr.getTopValue());
                for (Map.Entry<Set<Tag>,Aggregation> entry : samples.entrySet()) {
                    Set<Tag> allMatchingTags = (Set<Tag>) matchingTagCache.get(entry.getKey());
                    if (allMatchingTags == null) {
                        allMatchingTags = new HashSet<>();
                        for (Tag tag : entry.getKey()) {
                            if (query.getTags().keySet().contains(tag.getKey())) {
                                allMatchingTags.add(tag);
                            }
                        }
                        matchingTagCache.put(entry.getKey(), allMatchingTags);
                    }
                    List<Aggregation> aggregations = aggregationList.getOrDefault(allMatchingTags, new ArrayList<>());
                    aggregations.add(entry.getValue());
                    aggregationList.put(allMatchingTags, aggregations);
                }
                itr.next();
            }
            return aggregationList;
        } catch (Exception e) {
            log.error("Error during query: " + e.getMessage(), e);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: " + e.getMessage(), e.getMessage(), e);
        } finally {
            log.trace("Time for cache subquery for {} - {}ms", query.toString(), System.currentTimeMillis() - start);
        }
    }

    public List<MetricResponse> getMetricsFromCache(AuthenticatedRequest request, String metric, Map<String,String> tags, long begin, long end) {

        List<MetricResponse> metricResponses = new ArrayList<>();
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setMetric(metric);
        if (tags != null) {
            for (Map.Entry<String,String> t : tags.entrySet()) {
                subQuery.addTag(t.getKey(), t.getValue());
            }
        }
        Collection<Authorizations> authorizations = getSessionAuthorizations(request);
        Collection<Authorizations> minimizedAuths = AuthorizationsUtil.minimize(authorizations);
        Collection<VisibilityFilter> visibilityFilters = new ArrayList<>();
        for (Authorizations a : minimizedAuths) {
            visibilityFilters.add(new VisibilityFilter(a));
        }
        DataStoreCacheIterator itr = new DataStoreCacheIterator(this, visibilityFilters, subQuery, begin, end);
        try {
            itr.seek(new Range(subQuery.getMetric()), null, true);
            while (itr.hasTop()) {
                Key k = itr.getTopKey();
                Value v = itr.getTopValue();
                Metric m = MetricAdapter.parse(k, v);
                MetricResponse mr = new MetricResponse();
                mr.setMetric(metric);
                mr.setTags(m.getTags());
                mr.setTimestamp(m.getValue().getTimestamp());
                mr.setValue(m.getValue().getMeasure());
                metricResponses.add(mr);
                itr.next();
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
        return metricResponses;
    }

    public SortedKeyValueIterator<Key,Value> setupIterator(QueryRequest query, QueryRequest.SubQuery subQuery, Collection<Authorizations> authorizations,
                    long ageOffForMetric) throws TimelyException {

        SortedKeyValueIterator<org.apache.accumulo.core.data.Key,org.apache.accumulo.core.data.Value> itr = null;

        long downsamplePeriod = DownsampleIterator.getDownsamplePeriod(subQuery);
        long startTs = query.getStart();
        long endTs = query.getEnd();
        long ageOffTs = System.currentTimeMillis() - ageOffForMetric;
        if (startTs <= ageOffTs) {
            startTs = ageOffTs + 1;
        }

        long startOfFirstPeriod = startTs - (startTs % downsamplePeriod);
        long endDistanceFromDownSample = endTs % downsamplePeriod;
        long endOfLastPeriod = (endDistanceFromDownSample > 0 ? endTs + downsamplePeriod - endDistanceFromDownSample : endTs);

        try {
            // create DataStoreCacheIterator which is the base iterator of the stack
            Collection<Authorizations> minimizedAuths = AuthorizationsUtil.minimize(authorizations);
            Collection<VisibilityFilter> visibilityFilters = new ArrayList<>();
            for (Authorizations a : minimizedAuths) {
                visibilityFilters.add(new VisibilityFilter(a));
            }
            itr = new DataStoreCacheIterator(this, visibilityFilters, subQuery, startOfFirstPeriod, endOfLastPeriod);

            // create RateIterator if necessary
            if (subQuery.isRate()) {
                log.trace("Adding rate iterator");
                IteratorSetting rate = new IteratorSetting(499, RateIterator.class);
                RateIterator.setRateOptions(rate, subQuery.getRateOptions());
                RateIterator rateIterator = new RateIterator();
                rateIterator.init(itr, rate.getOptions(), null);
                itr = rateIterator;
            }

            // create DownsampleIterator - we should always have a downsample
            // iterator
            // in the stack even if only using the default downsample settings
            Class<? extends Aggregator> daggClass = DownsampleIterator.getDownsampleAggregator(subQuery);
            if (daggClass == null) {
                throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: programming error", "daggClass == null");
            } else {
                log.trace("Downsample Aggregator type {}", daggClass.getSimpleName());
                IteratorSetting downsample = new IteratorSetting(500, DownsampleIterator.class);
                DownsampleIterator.setDownsampleOptions(downsample, startOfFirstPeriod, endOfLastPeriod, DownsampleIterator.getDownsamplePeriod(subQuery), -1,
                                daggClass.getName());
                DownsampleIterator downsampleIterator = new DownsampleIterator();
                downsampleIterator.init(itr, downsample.getOptions(), null);
                itr = downsampleIterator;
            }

            // create AggregatingIterator if necessary
            Class<? extends Aggregator> aggClass = Aggregator.getAggregator(subQuery.getAggregator());
            // the aggregation iterator is optional
            if (aggClass != null) {
                log.trace("Aggregator type {}", aggClass.getSimpleName());
                IteratorSetting aggregation = new IteratorSetting(501, AggregationIterator.class);
                AggregationIterator.setAggregationOptions(aggregation, subQuery.getTags(), aggClass.getName());
                AggregationIterator aggregationIterator = new AggregationIterator();
                aggregationIterator.init(itr, aggregation.getOptions(), null);
                itr = aggregationIterator;
            }

            itr.seek(new Range(subQuery.getMetric()), null, true);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return itr;
    }

    private Collection<Authorizations> getSessionAuthorizations(AuthenticatedRequest request) {
        return authenticationService.getAuthorizations(request);
    }

    private QueryResponse convertToQueryResponse(QueryRequest.SubQuery query, Set<Tag> tags, Collection<Aggregation> values, long tsDivisor) {
        QueryResponse response = new QueryResponse();
        response.setMetric(query.getMetric());
        for (Tag tag : tags) {
            response.putTag(tag.getKey(), tag.getValue());
        }
        QueryRequest.RateOption rateOptions = query.getRateOptions();
        Aggregation combined = Aggregation.combineAggregation(values, rateOptions);
        for (Sample entry : combined) {
            long ts = entry.getTimestamp() / tsDivisor;
            response.putDps(Long.toString(ts), entry.getValue());
        }
        log.trace("Created query response {}", response);
        return response;
    }

    public long getNewestTimestamp(String metric) {
        return getNewestTimestamp(metric, 0);
    }

    public long getNewestTimestamp(String metric, long lockStamp) {

        long newest = 0;
        long stamp = lockStamp == 0 ? gorillaMapLock.readLock() : lockStamp;
        try {
            Map<TaggedMetric,GorillaStore> gorillaStoreMap = gorillaMap.get(metric);
            for (Map.Entry<TaggedMetric,GorillaStore> entry : gorillaStoreMap.entrySet()) {
                if (entry.getValue().getNewestTimestamp() > newest) {
                    newest = entry.getValue().getNewestTimestamp();
                }
            }
        } finally {
            if (lockStamp == 0) {
                gorillaMapLock.unlockRead(stamp);
            }
        }
        return newest;
    }

    public long getOldestTimestamp(String metric) {
        long oldest = Long.MAX_VALUE;
        long stamp = gorillaMapLock.readLock();
        try {
            Map<TaggedMetric,GorillaStore> gorillaStoreMap = gorillaMap.get(metric);
            if (gorillaStoreMap != null) {
                for (Map.Entry<TaggedMetric,GorillaStore> entry : gorillaStoreMap.entrySet()) {
                    if (entry.getValue().getOldestTimestamp() < oldest) {
                        oldest = entry.getValue().getOldestTimestamp();
                    }
                }
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
        return oldest;
    }

    public long getNewestTimestamp() {

        long newest = 0;
        long stamp = gorillaMapLock.readLock();
        try {
            for (String metric : gorillaMap.keySet()) {
                Long newestForMetric = getNewestTimestamp(metric);
                if (newestForMetric > newest) {
                    newest = newestForMetric;
                }
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
        return newest;
    }

    public long getOldestTimestamp() {
        long oldest = Long.MAX_VALUE;
        long stamp = gorillaMapLock.readLock();
        try {
            for (String metric : gorillaMap.keySet()) {
                Long oldestForMetric = getOldestTimestamp(metric);
                if (oldestForMetric < oldest) {
                    oldest = oldestForMetric;
                }
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
        return oldest;
    }

    public CacheResponse getCacheStatus() {
        CacheResponse response = new CacheResponse();
        response.setOldestTimestamp(getOldestTimestamp());
        response.setNewestTimestamp(getNewestTimestamp());
        long stamp = gorillaMapLock.readLock();
        try {
            response.setMetrics(new ArrayList<>(gorillaMap.keySet()));
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
        return response;
    }

    public void clear() {
        long stamp = gorillaMapLock.readLock();
        try {
            gorillaMap.clear();
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
    }
}

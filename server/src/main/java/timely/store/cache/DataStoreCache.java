package timely.store.cache;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.CacheResponse;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.auth.AuthCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.sample.Aggregation;
import timely.sample.Aggregator;
import timely.sample.Sample;
import timely.sample.iterators.AggregationIterator;
import timely.sample.iterators.DownsampleIterator;
import timely.store.MetricAgeOffIterator;
import timely.store.iterators.RateIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.locks.StampedLock;

public class DataStoreCache {

    private static final Logger LOG = LoggerFactory.getLogger(DataStoreCache.class);
    public static final String DEFAULT_AGEOFF_KEY = "default";

    private Map<String, Map<TaggedMetric, GorillaStore>> gorillaMap = new HashMap<>();
    private StampedLock gorillaMapLock = new StampedLock();

    private Set<String> nonCachedMetrics = Collections.synchronizedSet(new HashSet<>());
    private long maxUniqueTagSets;
    private boolean anonAccessAllowed;
    private Map<String, Long> minimumAgeOff;
    private Map<String, String> minimumAgeOffForIterator;
    private int flushBatch = 0;
    private int numBatches = 5;

    private Timer maintenanceTimer = new Timer("DataStoreCacheTimer");
    private Timer statsTimer = new Timer();

    public DataStoreCache(Configuration conf) {
        nonCachedMetrics.addAll(conf.getCache().getNonCachedMetrics());
        maxUniqueTagSets = conf.getCache().getMaxUniqueTagSets();
        anonAccessAllowed = conf.getSecurity().isAllowAnonymousAccess();
        Map<String, Integer> cacheAgeOff = conf.getCache().getMetricAgeOffHours();
        Map<String, Integer> accumuloAgeOff = conf.getMetricAgeOffDays();
        LOG.info("cacheAgeOff:{} accumuloAgeOff:{}", cacheAgeOff, accumuloAgeOff);
        minimumAgeOff = getMinimumAgeOffs(accumuloAgeOff, cacheAgeOff);

        for (Map.Entry<String, Long> e : minimumAgeOff.entrySet()) {
            LOG.info("minimumAgeOff key:{} value:{}", e.getKey(), e.getValue());
        }

        minimumAgeOffForIterator = getAgeOffForIterator(minimumAgeOff);

        maintenanceTimer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    flushCaches(flushBatch);
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }, conf.getCache().getFlushInterval(), conf.getCache().getFlushInterval());

        maintenanceTimer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    ageOffGorillaStores();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }, (600 * 1000), (600 * 1000));

        maintenanceTimer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    archiveGorillaStoreCurrentCompressors();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }, (7200 * 1000), (3600 * 100));

        statsTimer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    pruneStats();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }, (600 * 1000), (600 * 100));
    }

    private void pruneStats() {
        long stamp = gorillaMapLock.writeLock();
        try {
            Iterator<Map.Entry<String, Map<TaggedMetric, GorillaStore>>> metricItr = gorillaMap.entrySet().iterator();

            while (metricItr.hasNext()) {
                Map.Entry<String, Map<TaggedMetric, GorillaStore>> entry1 = metricItr.next();
                int numberTagVariations = entry1.getValue().size();
                if (numberTagVariations > maxUniqueTagSets) {
                    LOG.info("Cache of metric {} has {] tag variations.  Discontinuing cache.", entry1.getKey(),
                            numberTagVariations);
                    metricItr.remove();
                    nonCachedMetrics.add(entry1.getKey());
                }
            }
        } finally {
            gorillaMapLock.unlockWrite(stamp);
        }
    }

    private void ageOffGorillaStores() {
        long stamp = gorillaMapLock.readLock();
        try {
            long numRemoved = 0;
            for (Map.Entry<String, Map<TaggedMetric, GorillaStore>> entry1 : gorillaMap.entrySet()) {
                long maxAge = getAgeOffForMetric(entry1.getKey());
                if (LOG.isTraceEnabled()) {
                    LOG.trace("ageOffGorillaStores metric:{} maxAge:{}", entry1.getKey(), maxAge);
                }
                for (GorillaStore store : entry1.getValue().values()) {
                    numRemoved += store.ageOffArchivedCompressors(maxAge);
                }
            }
            if (numRemoved > 0) {
                LOG.debug("ageOffGorillaStores - Aged off {} archived Gorilla compressors", numRemoved);
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
    }

    protected void flushCaches(int flushBatch) {
        long stamp = gorillaMapLock.readLock();
        try {
            int x = 0;
            for (Map.Entry<String, Map<TaggedMetric, GorillaStore>> entry : gorillaMap.entrySet()) {
                if (flushBatch == -1 || x % numBatches == flushBatch) {
                    for (GorillaStore store : entry.getValue().values()) {
                        try {
                            store.flush();
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                        }
                    }
                }
                x++;
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
            flushBatch++;
            if (flushBatch == numBatches) {
                flushBatch = 0;
            }
        }
    }

    private void archiveGorillaStoreCurrentCompressors() {
        long stamp = gorillaMapLock.readLock();
        try {
            for (Map.Entry<String, Map<TaggedMetric, GorillaStore>> entry : gorillaMap.entrySet()) {
                for (GorillaStore store : entry.getValue().values()) {
                    store.archiveCurrentCompressor();
                }
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
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
        this.minimumAgeOffForIterator.put(
                MetricAgeOffIterator.AGE_OFF_PREFIX + MetricAgeOffIterator.DEFAULT_AGEOFF_KEY,
                Long.toString(defaultAgeOffMilliSec));
    }

    private Map<String, Long> getMinimumAgeOffs(Map<String, Integer> accumuloAgeOffDays,
            Map<String, Integer> cacheAgeOffHours) {
        Map<String, Long> minimumAgeOffs = new HashMap<>();
        Set<String> keys = new HashSet<>();
        keys.addAll(accumuloAgeOffDays.keySet());
        keys.addAll(cacheAgeOffHours.keySet());
        for (String name : keys) {
            Long accumuloAgeOffValue = (accumuloAgeOffDays.containsKey(name)) ? accumuloAgeOffDays.get(name) * 86400000L
                    : Long.MAX_VALUE;
            Long cacheAgeOffValue = (cacheAgeOffHours.containsKey(name)) ? cacheAgeOffHours.get(name) * 3600000L
                    : Long.MAX_VALUE;
            minimumAgeOffs.put(name, Math.min(accumuloAgeOffValue, cacheAgeOffValue));
        }
        return minimumAgeOffs;
    }

    private Map<String, String> getAgeOffForIterator(Map<String, Long> minimumAgeOff) {
        Map<String, String> ageOffOptions = new HashMap<>();
        minimumAgeOff.forEach((k, v) -> {
            String ageoff = Long.toString(v);
            LOG.trace("Adding age off for metric: {} of {} milliseconds", k, v);
            ageOffOptions.put(MetricAgeOffIterator.AGE_OFF_PREFIX + k, ageoff);
        });
        ageOffOptions
                .put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, Long.toString(minimumAgeOff.get(DEFAULT_AGEOFF_KEY)));
        return ageOffOptions;
    }

    public Map<TaggedMetric, GorillaStore> getGorillaStores(String metric) {
        Map<TaggedMetric, GorillaStore> returnedMap = new HashMap<>();
        Map<TaggedMetric, GorillaStore> metricMap;
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
        } finally {
            gorillaMapLock.unlock(stamp);
        }
        returnedMap.putAll(metricMap);
        return returnedMap;
    }

    public GorillaStore getGorillaStore(String metric, TaggedMetric taggedMetric) {

        Map<TaggedMetric, GorillaStore> metricMap;
        GorillaStore gStore;
        boolean needWrite = false;
        long stamp = gorillaMapLock.readLock();
        try {
            metricMap = gorillaMap.get(metric);
            if (metricMap == null) {
                metricMap = new HashMap<>();
                needWrite = true;
            }
            gStore = metricMap.get(taggedMetric);
            if (gStore == null) {
                gStore = new GorillaStore();
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
        synchronized (nonCachedMetrics) {
            for (String r : nonCachedMetrics) {
                if (metricName.matches(r)) {
                    LOG.info("Adding {} to list of non-cached metrics", metricName);
                    nonCachedMetrics.add(metricName);
                    return false;
                }
            }
        }
        return true;
    }

    public void store(Metric metric) {
        if (shouldCache(metric)) {
            TaggedMetric taggedMetric = new TaggedMetric(metric.getName(), metric.getTags());
            GorillaStore gs = getGorillaStore(metric.getName(), taggedMetric);
            gs.addValue(metric);
        }
    }

    public List<QueryResponse> query(QueryRequest msg) throws TimelyException {

        List<QueryResponse> result = new ArrayList<>();
        try {
            Collection<QueryRequest.SubQuery> subQueries = msg.getQueries();
            for (QueryRequest.SubQuery query : subQueries) {
                Map<Set<Tag>, List<Aggregation>> aggregations = subquery(msg, query);
                for (Map.Entry<Set<Tag>, List<Aggregation>> entry : aggregations.entrySet()) {
                    long tsDivisor = msg.isMsResolution() ? 1 : 1000;
                    result.add(convertToQueryResponse(query, entry.getKey(), entry.getValue(), tsDivisor));
                }
            }
            return result;
        } catch (Exception e) {
            LOG.error("Error during query: " + e.getMessage(), e);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: "
                    + e.getMessage(), e.getMessage(), e);
        }
    }

    public Map<Set<Tag>, List<Aggregation>> subquery(QueryRequest msg, QueryRequest.SubQuery query)
            throws TimelyException {

        Map<Set<Tag>, List<Aggregation>> aggregationList = new HashMap<>();
        long start = System.currentTimeMillis();
        try {
            SortedKeyValueIterator<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> itr = null;
            itr = setupIterator(msg, query, getSessionAuthorizations(msg), getAgeOffForMetric(query.getMetric()));
            Map<Set<Tag>, Set<Tag>> matchingTagCache = new HashMap<>();
            while (itr.hasTop()) {
                Map<Set<Tag>, Aggregation> samples = AggregationIterator.decodeValue(itr.getTopValue());
                for (Map.Entry<Set<Tag>, Aggregation> entry : samples.entrySet()) {
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
            LOG.error("Error during query: " + e.getMessage(), e);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: "
                    + e.getMessage(), e.getMessage(), e);
        } finally {
            LOG.trace("Time for cache subquery for {} - {}ms", query.toString(), System.currentTimeMillis() - start);
        }
    }

    protected SortedKeyValueIterator<Key, Value> setupIterator(QueryRequest query, QueryRequest.SubQuery subQuery,
            Authorizations authorizations, long ageOffForMetric) throws TimelyException {

        SortedKeyValueIterator<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> itr = null;

        long downsamplePeriod = DownsampleIterator.getDownsamplePeriod(subQuery);
        long startTs = query.getStart();
        long endTs = query.getEnd();
        long ageOffTs = System.currentTimeMillis() - ageOffForMetric;
        if (startTs <= ageOffTs) {
            startTs = ageOffTs + 1;
        }

        long startOfFirstPeriod = startTs - (startTs % downsamplePeriod);
        long endDistanceFromDownSample = endTs % downsamplePeriod;
        long endOfLastPeriod = (endDistanceFromDownSample > 0 ? endTs + downsamplePeriod - endDistanceFromDownSample
                : endTs);

        try {
            // create DataStoreCacheIterator which is the base iterator of
            // the stack
            VisibilityFilter visFilter = new VisibilityFilter(authorizations);

            itr = new DataStoreCacheIterator(this, visFilter, subQuery, startOfFirstPeriod, endOfLastPeriod);

            // IteratorSetting ageOffIteratorSettings = new IteratorSetting(100,
            // "ageoff", MetricAgeOffIterator.class,
            // this.minimumAgeOffForIterator);
            // MetricAgeOffIterator ageOff = new MetricAgeOffIterator();
            // ageOff.init(itr, ageOffIteratorSettings.getOptions(), null);
            // itr = ageOff;

            // create RateIterator if necessary
            if (subQuery.isRate()) {
                LOG.trace("Adding rate iterator");
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
                throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                        "Error during query: programming error", "daggClass == null");
            } else {
                LOG.trace("Downsample Aggregator type {}", daggClass.getSimpleName());
                IteratorSetting downsample = new IteratorSetting(500, DownsampleIterator.class);
                DownsampleIterator.setDownsampleOptions(downsample, startOfFirstPeriod, endOfLastPeriod,
                        DownsampleIterator.getDownsamplePeriod(subQuery), -1, daggClass.getName());
                DownsampleIterator downsampleIterator = new DownsampleIterator();
                downsampleIterator.init(itr, downsample.getOptions(), null);
                itr = downsampleIterator;
            }

            // create AggregatingIterator if necessary
            Class<? extends Aggregator> aggClass = Aggregator.getAggregator(subQuery.getAggregator());
            // the aggregation iterator is optional
            if (aggClass != null) {
                LOG.trace("Aggregator type {}", aggClass.getSimpleName());
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

    private Authorizations getSessionAuthorizations(AuthenticatedRequest request) {
        return getSessionAuthorizations(request.getSessionId());
    }

    private Authorizations getSessionAuthorizations(String sessionId) {
        if (anonAccessAllowed) {
            if (StringUtils.isEmpty(sessionId)) {
                return Authorizations.EMPTY;
            } else {
                Authorizations auths = AuthCache.getAuthorizations(sessionId);
                if (null == auths) {
                    auths = Authorizations.EMPTY;
                }
                return auths;
            }
        } else {
            if (StringUtils.isEmpty(sessionId)) {
                throw new IllegalArgumentException("session id cannot be null");
            } else {
                Authorizations auths = AuthCache.getAuthorizations(sessionId);
                if (null == auths) {
                    throw new IllegalStateException("No auths found for sessionId: " + sessionId);
                }
                return auths;
            }
        }
    }

    private QueryResponse convertToQueryResponse(QueryRequest.SubQuery query, Set<Tag> tags,
            Collection<Aggregation> values, long tsDivisor) {
        QueryResponse response = new QueryResponse();
        response.setMetric(query.getMetric());
        for (Tag tag : tags) {
            response.putTag(tag.getKey(), tag.getValue());
        }
        QueryRequest.RateOption rateOptions = query.getRateOptions();
        Aggregation combined = Aggregation.combineAggregation(values, rateOptions);
        for (Sample entry : combined) {
            long ts = entry.timestamp / tsDivisor;
            response.putDps(Long.toString(ts), entry.value);
        }
        LOG.trace("Created query response {}", response);
        return response;
    }

    public long getNewestTimestamp(String metric) {

        long newest = 0;
        long stamp = gorillaMapLock.readLock();
        try {
            Map<TaggedMetric, GorillaStore> gorillaStoreMap = gorillaMap.get(metric);
            for (Map.Entry<TaggedMetric, GorillaStore> entry : gorillaStoreMap.entrySet()) {
                if (entry.getValue().getNewestTimestamp() > newest) {
                    newest = entry.getValue().getNewestTimestamp();
                }
            }
        } finally {
            gorillaMapLock.unlockRead(stamp);
        }
        return newest;
    }

    public long getOldestTimestamp(String metric) {
        long oldest = Long.MAX_VALUE;
        long stamp = gorillaMapLock.readLock();
        try {
            Map<TaggedMetric, GorillaStore> gorillaStoreMap = gorillaMap.get(metric);
            if (gorillaStoreMap != null) {
                for (Map.Entry<TaggedMetric, GorillaStore> entry : gorillaStoreMap.entrySet()) {
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
}

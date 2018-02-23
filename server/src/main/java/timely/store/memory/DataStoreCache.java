package timely.store.memory;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.request.timeseries.SuggestRequest;
import timely.api.response.CacheResponse;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.api.response.timeseries.SearchLookupResponse;
import timely.api.response.timeseries.SuggestResponse;
import timely.auth.AuthCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.sample.Aggregation;
import timely.sample.Aggregator;
import timely.sample.Sample;
import timely.sample.aggregators.Avg;
import timely.sample.iterators.AggregationIterator;
import timely.sample.iterators.DownsampleIterator;
import timely.store.DataStore;
import timely.store.MetricAgeOffIterator;
import timely.store.iterators.RateIterator;

import java.io.IOException;
import java.util.*;

public class DataStoreCache implements DataStore {

    private static final Logger LOG = LoggerFactory.getLogger(DataStoreCache.class);

    private Map<String, Map<TaggedMetric, GorillaStore>> gorillaMap = new HashMap<>();
    private boolean anonAccessAllowed = false;
    private long ageOffMsec;
    private Map<String, String> ageOff = new HashMap<>();
    private long defaultAgeOff = Long.MAX_VALUE;

    public DataStoreCache(Configuration conf) throws TimelyException {

        anonAccessAllowed = conf.getSecurity().isAllowAnonymousAccess();
        ageOffMsec = conf.getCache().getExpirationMinutes();
    }

    private long getAgeOffForMetric(String metricName) {
        String age = this.ageOff.get(MetricAgeOffIterator.AGE_OFF_PREFIX + metricName);
        if (null == age) {
            return this.defaultAgeOff;
        } else {
            return Long.parseLong(age);
        }
    }

    public void setAgeOff(Map<String, String> ageOff) {
        this.ageOff = ageOff;
    }

    public void setDefaultAgeOff(long defaultAgeOff) {
        this.defaultAgeOff = defaultAgeOff;
    }

    protected Map<TaggedMetric, GorillaStore> getGorillaStores(String metric) {
        Map<TaggedMetric, GorillaStore> metricMap = gorillaMap.get(metric);
        if (metricMap == null) {
            metricMap = new HashMap<>();
            gorillaMap.put(metric, metricMap);
        }
        return metricMap;
    }

    private GorillaStore getGorillaStore(TaggedMetric taggedMetric) {
        Map<TaggedMetric, GorillaStore> metricMap = gorillaMap.get(taggedMetric.getMetric());
        if (metricMap == null) {
            metricMap = new HashMap<>();
            gorillaMap.put(taggedMetric.getMetric(), metricMap);
        }
        GorillaStore gStore = metricMap.get(taggedMetric);
        if (gStore == null) {
            gStore = new GorillaStore();
            metricMap.put(taggedMetric, gStore);
        }
        return gStore;
    }

    public void store(Metric metric) {

        TaggedMetric taggedMetric = new TaggedMetric(metric.getName(), metric.getTags());
        getGorillaStore(taggedMetric).addValue(metric.getValue().getTimestamp(), metric.getValue().getMeasure());
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

        try {
            SortedKeyValueIterator<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> itr = null;
            itr = setupIterator(msg, query, getSessionAuthorizations(msg), getAgeOffForMetric(query.getMetric()));

            while (itr.hasTop()) {
                Map<Set<Tag>, Aggregation> samples = AggregationIterator.decodeValue(itr.getTopValue());
                for (Map.Entry<Set<Tag>, Aggregation> entry : samples.entrySet()) {
                    Set<Tag> key = new HashSet<>();
                    for (Tag tag : entry.getKey()) {
                        if (query.getTags().keySet().contains(tag.getKey())) {
                            key.add(tag);
                        }
                    }
                    List<Aggregation> aggregations = aggregationList.getOrDefault(key, new ArrayList<>());
                    aggregations.add(entry.getValue());
                    aggregationList.put(key, aggregations);
                }
                itr.next();
            }
            return aggregationList;
        } catch (Exception e) {
            LOG.error("Error during query: " + e.getMessage(), e);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: "
                    + e.getMessage(), e.getMessage(), e);
        }
    }

    @Override
    public void flush() throws TimelyException {

    }

    protected SortedKeyValueIterator<Key, Value> setupIterator(QueryRequest query, QueryRequest.SubQuery subQuery,
            Authorizations authorizations, long ageOffForMetric) throws TimelyException {

        SortedKeyValueIterator<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> itr = null;
        try {
            // create MetricMemoryStoreIterator which is the base iterator of
            // the stack
            VisibilityFilter visFilter = new VisibilityFilter(authorizations);
            long ageOffTs = System.currentTimeMillis() - ageOffForMetric;
            long startTs = query.getStart();
            if (startTs <= ageOffTs) {
                startTs = ageOffTs + 1;
            }
            itr = new MetricMemoryStoreIterator(this, visFilter, subQuery, startTs, query.getEnd());

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
                DownsampleIterator.setDownsampleOptions(downsample, query.getStart(), query.getEnd(),
                        DownsampleIterator.getDownsamplePeriod(subQuery), -1, Avg.class.getName());
                DownsampleIterator downsampleIterator = new DownsampleIterator();
                downsampleIterator.init(itr, downsample.getOptions(), null);
                itr = downsampleIterator;
            }

            // create AggregatingIterator if necessary
            Class<? extends Aggregator> aggClass = Aggregator.getAggregator(subQuery.getAggregator());
            // the aggregation iterator is optional
            if (aggClass != null) {
                LOG.trace("Aggregator type {}", aggClass.getSimpleName());
                IteratorSetting downsample = new IteratorSetting(501, AggregationIterator.class);
                AggregationIterator.setAggregationOptions(downsample, subQuery.getTags(), aggClass.getName());
                DownsampleIterator downsampleIterator = new DownsampleIterator();
                downsampleIterator.init(itr, downsample.getOptions(), null);
                itr = downsampleIterator;
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

    @Override
    public SuggestResponse suggest(SuggestRequest query) throws TimelyException {
        throw new TimelyException(500, "suggest not implemented", "suggest not implemented in "
                + this.getClass().getSimpleName());
    }

    @Override
    public SearchLookupResponse lookup(SearchLookupRequest msg) throws TimelyException {
        throw new TimelyException(500, "lookup not implemented", "lookup not implemented in "
                + this.getClass().getSimpleName());
    }

    @Override
    public Scanner createScannerForMetric(String sessionId, String metric, Map<String, String> tags, long startTime,
            long endTime, int lag, int scannerBatchSize, int scannerReadAhead) throws TimelyException {
        throw new TimelyException(500, "createScannerForMetric not implemented",
                "createScannerForMetric not implemented in " + this.getClass().getSimpleName());
    }

    public long getNewestTimestamp(String metric) {

        long newest = 0;
        Map<TaggedMetric, GorillaStore> gorillaStoreMap = gorillaMap.get(metric);
        for (Map.Entry<TaggedMetric, GorillaStore> entry : gorillaStoreMap.entrySet()) {
            if (entry.getValue().getNewestTimestamp() > newest) {
                newest = entry.getValue().getNewestTimestamp();
            }
        }
        return newest;
    }

    public long getOldestTimestamp(String metric) {
        long oldest = Long.MAX_VALUE;
        Map<TaggedMetric, GorillaStore> gorillaStoreMap = gorillaMap.get(metric);
        for (Map.Entry<TaggedMetric, GorillaStore> entry : gorillaStoreMap.entrySet()) {
            if (entry.getValue().getOldestTimestamp() < oldest) {
                oldest = entry.getValue().getOldestTimestamp();
            }
        }
        return oldest;
    }

    public long getNewestTimestamp() {

        long newest = 0;
        for (String metric : gorillaMap.keySet()) {
            Long newestForMetric = getNewestTimestamp(metric);
            if (newestForMetric > newest) {
                newest = newestForMetric;
            }
        }
        return newest;
    }

    public long getOldestTimestamp() {
        long oldest = Long.MAX_VALUE;
        for (String metric : gorillaMap.keySet()) {
            Long oldestForMetric = getOldestTimestamp(metric);
            if (oldestForMetric < oldest) {
                oldest = oldestForMetric;
            }
        }
        return oldest;
    }

    public CacheResponse getCacheStatus() {
        CacheResponse response = new CacheResponse();
        response.setOldestTimestamp(getOldestTimestamp());
        response.setNewestTimestamp(getNewestTimestamp());
        response.setMetrics(new ArrayList<>(gorillaMap.keySet()));
        return response;
    }

    public long getAgeOffMsec() {
        return ageOffMsec;
    }
}

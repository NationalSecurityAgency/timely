package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.accumulo.core.client.IteratorSetting;
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
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.auth.AuthCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.sample.Aggregation;
import timely.sample.Aggregator;
import timely.sample.Downsample;
import timely.sample.Sample;
import timely.sample.aggregators.Avg;
import timely.sample.iterators.AggregationIterator;
import timely.sample.iterators.DownsampleIterator;
import timely.store.iterators.RateIterator;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import static org.apache.accumulo.core.conf.AccumuloConfiguration.getTimeInMillis;

public class MetricMemoryStore {

    private static final long DEFAULT_DOWNSAMPLE_MS = 1;
    private static final String DEFAULT_DOWNSAMPLE_AGGREGATOR = Avg.class.getSimpleName().toLowerCase();
    private static final Logger LOG = LoggerFactory.getLogger(MetricMemoryStore.class);

    private Map<String, Map<TaggedMetric, GorillaStore>> gorillaMap = new HashMap<>();
    private boolean anonAccessAllowed = false;

    public MetricMemoryStore(Configuration conf) throws TimelyException {

        anonAccessAllowed = conf.getSecurity().isAllowAnonymousAccess();
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
            SortedKeyValueIterator<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> itr = null;
            Collection<QueryRequest.SubQuery> subQueries = msg.getQueries();
            for (QueryRequest.SubQuery query : subQueries) {
                itr = setupIterator(msg, query, getSessionAuthorizations(msg));

                while (itr.hasTop()) {
                    itr.next();
                    Map<Set<Tag>, Aggregation> aggregations = AggregationIterator.decodeValue(itr.getTopValue());
                    for (Map.Entry<Set<Tag>, Aggregation> entry : aggregations.entrySet()) {
                        long tsDivisor = msg.isMsResolution() ? 1 : 1000;
                        result.add(convertToQueryResponse(query, entry.getKey(),
                                Collections.singleton(entry.getValue()), tsDivisor));
                    }
                }
            }
            return result;
        } catch (Exception e) {
            LOG.error("Error during query: " + e.getMessage(), e);
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error during query: "
                    + e.getMessage(), e.getMessage(), e);
        }
    }

    protected SortedKeyValueIterator<Key, Value> setupIterator(QueryRequest query, QueryRequest.SubQuery subQuery,
            Authorizations authorizations) throws TimelyException {

        SortedKeyValueIterator<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> itr = null;
        try {
            // create MetricMemoryStoreIterator which is the base iterator of
            // the stack
            VisibilityFilter visFilter = new VisibilityFilter(authorizations);
            itr = new MetricMemoryStoreIterator(this, visFilter, subQuery, 0, 86400000);

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

    static public class CustomComparator implements Comparator<Pair>, Serializable {

        @Override
        public int compare(Pair p1, Pair p2) {
            if (p1.getTimestamp() == p2.getTimestamp()) {
                if (Double.compare(p1.getDoubleValue(), p2.getDoubleValue()) == 0) {
                    return 0;
                } else {
                    return Double.compare(p1.getDoubleValue(), p2.getDoubleValue());
                }
            } else {
                return (p1.getTimestamp() < p2.getTimestamp()) ? -1 : 1;
            }
        }
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

    private QueryResponse convertToQueryResponse2(QueryRequest.SubQuery query, Map<String, String> tags,
            Aggregation values, long tsDivisor) {
        QueryResponse response = new QueryResponse();
        Set<String> requestedTags = query.getTags().keySet();
        response.setMetric(query.getMetric());
        for (Map.Entry<String, String> tag : tags.entrySet()) {
            if (requestedTags.contains(tag.getKey())) {
                response.putTag(tag.getKey(), tag.getValue());
            }
        }
        // QueryRequest.RateOption rateOptions = query.getRateOptions();
        // Aggregation combined = Aggregation.combineAggregation(values,
        // rateOptions);
        for (Sample entry : values) {
            long ts = entry.timestamp / tsDivisor;
            response.putDps(Long.toString(ts), entry.value);
        }
        return response;
    }
}

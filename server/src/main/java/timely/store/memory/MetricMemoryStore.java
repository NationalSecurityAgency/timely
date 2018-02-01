package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;
import io.netty.handler.codec.http.HttpResponseStatus;
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
import timely.sample.Aggregation;
import timely.sample.Aggregator;
import timely.sample.Downsample;
import timely.sample.Sample;
import timely.sample.aggregators.Avg;

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

            long startTs = msg.getStart();
            long endTs = msg.getEnd();
            Authorizations auths = getSessionAuthorizations(msg);
            VisibilityFilter visibilityFilter = new VisibilityFilter(auths);

            Collection<QueryRequest.SubQuery> subQueries = msg.getQueries();
            for (QueryRequest.SubQuery query : subQueries) {

                String metric = query.getMetric();
                Map<String, String> tags = query.getTags();
                Map<TaggedMetric, GorillaStore> gorillaStores = getGorillaStores(metric);
                for (Map.Entry<TaggedMetric, GorillaStore> entry : gorillaStores.entrySet()) {
                    Downsample downsample = getDownsample(startTs, endTs, query);
                    TaggedMetric storedTaggedMetric = entry.getKey();
                    if (storedTaggedMetric.matches(tags) && storedTaggedMetric.isVisible(visibilityFilter)) {
                        GorillaDecompressor decompressor = entry.getValue().getDecompressor();
                        Pair pair = null;
                        while ((pair = decompressor.readPair()) != null) {
                            if (pair.getTimestamp() >= startTs && pair.getTimestamp() <= endTs) {
                                downsample.add(pair.getTimestamp(), pair.getDoubleValue());
                            }
                        }
                        long tsDivisor = msg.isMsResolution() ? 1 : 1000;
                        result.add(convertToQueryResponse(query, storedTaggedMetric.getTags(), downsample, tsDivisor));
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

    private Downsample getDownsample(long startTs, long endTs, QueryRequest.SubQuery query) throws TimelyException {
        long downsample = getDownsamplePeriod(query);
        long startOfFirstPeriod = startTs - (startTs % downsample);
        long endDistanceFromDownSample = endTs % downsample;
        long endOfLastPeriod = (endDistanceFromDownSample > 0 ? endTs + downsample - endDistanceFromDownSample : endTs);
        Class<? extends Aggregator> daggClass = getDownsampleAggregator(query);

        Aggregator dagg = null;
        try {
            dagg = daggClass.newInstance();
            return new Downsample(startOfFirstPeriod, endOfLastPeriod, downsample, dagg);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "Error during query: programming error", "daggClass == null");
        }
    }

    private Class<? extends Aggregator> getAggregator(QueryRequest.SubQuery query) {
        return Aggregator.getAggregator(query.getAggregator());
    }

    private Class<? extends Aggregator> getDownsampleAggregator(QueryRequest.SubQuery query) {
        String aggregatorName = Aggregator.NONE;
        if (query.getDownsample().isPresent()) {
            String parts[] = query.getDownsample().get().split("-");
            aggregatorName = parts[1];
        }
        // disabling the downsampling OR setting the aggregation to none are
        // both considered to be disabling
        if (aggregatorName.equals(Aggregator.NONE)) {
            // we need a downsampling iterator, so default to max to ensure we
            // return something
            aggregatorName = DEFAULT_DOWNSAMPLE_AGGREGATOR;
        }
        return Aggregator.getAggregator(aggregatorName);
    }

    private long getDownsamplePeriod(QueryRequest.SubQuery query) {
        // disabling the downsampling OR setting the aggregation to none are
        // both considered to be disabling
        if (!query.getDownsample().isPresent() || query.getDownsample().get().endsWith("-none")) {
            return DEFAULT_DOWNSAMPLE_MS;
        }
        String parts[] = query.getDownsample().get().split("-");
        return getTimeInMillis(parts[0]);
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

    private QueryResponse convertToQueryResponse(QueryRequest.SubQuery query, Map<String, String> tags,
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

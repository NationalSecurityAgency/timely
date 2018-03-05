package timely.store.cache;

import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.model.Metric;
import timely.model.Tag;
import timely.model.Value;
import timely.sample.Aggregation;
import timely.sample.Sample;
import timely.sample.iterators.AggregationIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

public class TestDataStoreCacheIterator {

    private static final Logger LOG = LoggerFactory.getLogger(TestDataStoreCacheIterator.class);
    private static Configuration configuration = null;

    @BeforeClass
    public static void setup() {
        configuration = new Configuration();
        configuration.getSecurity().setAllowAnonymousAccess(true);
        HashMap<String, Integer> ageoffs = new HashMap<>();
        ageoffs.put(DataStoreCache.DEFAULT_AGEOFF_KEY, Integer.MAX_VALUE);
        configuration.getCache().setMetricAgeOffHours(ageoffs);
    }

    private DataStoreCache getMetricMemoryStore1(long baseTime) throws TimelyException {

        DataStoreCache mmStore = new DataStoreCache(configuration);

        Map<String, String> tags = new HashMap<>();
        tags.put("part", "webservice");
        tags.put("instance", "sample");

        Random r = new Random();
        int value = 0;
        for (int x = 0; x <= 60 * 24; x++) {
            for (int y = 0; y < 2; y++) {
                tags.put("host", (y % 2 == 0) ? "r01n01" : "r02n01");
                tags.put("rack", (y % 2 == 0) ? "r01" : "r02");
                value = r.nextInt(1000);
                mmStore.store(createMetric("metric.number.1", tags, value, baseTime + (x * 1000)));
                mmStore.store(createMetric("metric.number.2", tags, value, baseTime + (x * 1000)));
                mmStore.store(createMetric("metric.number.3", tags, value, baseTime + (x * 1000)));
                mmStore.store(createMetric("metric.number.4", tags, value, baseTime + (x * 1000)));
                mmStore.store(createMetric("metric.number.5", tags, value, baseTime + (x * 1000)));
            }
        }
        return mmStore;
    }

    private DataStoreCache getMetricMemoryStore2(long baseTime) throws TimelyException {

        DataStoreCache mmStore = new DataStoreCache(configuration);

        int increment = 10;
        Map<String, String> tags = new HashMap<>();
        tags.put("part", "webservice");
        tags.put("instance", "sample");

        int value = 0;
        for (int x = 0; x <= 60 * 24; x++) {
            for (int y = 0; y < 2; y++) {
                tags.put("host", (y % 2 == 0) ? "r01n01" : "r02n01");
                tags.put("rack", (y % 2 == 0) ? "r01" : "r02");
                if (y % 2 == 0) {
                    value += increment;
                } else {
                    value += 2 * increment;
                }
                mmStore.store(createMetric("metric.number.1", tags, value, baseTime + (x * 1000)));
                mmStore.store(createMetric("metric.number.2", tags, value, baseTime + (x * 1000)));
                mmStore.store(createMetric("metric.number.3", tags, value, baseTime + (x * 1000)));
                mmStore.store(createMetric("metric.number.4", tags, value, baseTime + (x * 1000)));
                mmStore.store(createMetric("metric.number.5", tags, value, baseTime + (x * 1000)));
            }
        }
        return mmStore;
    }

    @Test
    public void testDownsampleIterator() throws TimelyException {

        long BASETIME = System.currentTimeMillis();
        // align basetime to a downsample period
        BASETIME = BASETIME - (BASETIME % (1000 * 60));
        DataStoreCache mmStore = getMetricMemoryStore1(BASETIME);

        QueryRequest query = new QueryRequest();
        query.setStart(BASETIME);
        query.setEnd(BASETIME + 1440000);
        query.setMsResolution(true);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setDownsample(Optional.of("1m-avg"));
        subQuery.setMetric("metric.number.1");
        subQuery.addTag("host", ".*");
        query.setQueries(Collections.singleton(subQuery));

        SortedKeyValueIterator<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> itr = null;
        try {
            long firstTimestamp = -1;
            long lastTimestamp = -1;
            int numSamples = 0;
            itr = mmStore.setupIterator(query, subQuery, new Authorizations(), Long.MAX_VALUE);
            while (itr.hasTop()) {
                itr.next();
                Map<Set<Tag>, Aggregation> aggregations = AggregationIterator.decodeValue(itr.getTopValue());
                for (Map.Entry<Set<Tag>, Aggregation> entry : aggregations.entrySet()) {
                    for (Sample s : entry.getValue()) {
                        numSamples++;
                        if (firstTimestamp == -1) {
                            firstTimestamp = s.timestamp;
                        }
                        lastTimestamp = s.timestamp;
                    }
                }
            }
            Assert.assertEquals("First timestamp incorrect", BASETIME, firstTimestamp);
            Assert.assertEquals("Last timestamp incorrect", BASETIME + 1440000, lastTimestamp);
            Assert.assertEquals("Number of samples incorrect", 50, numSamples);
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("exception in test", e);
        }
    }

    @Test
    public void testRateIterator() throws TimelyException {

        long BASETIME = System.currentTimeMillis();
        // align basetime to a downsample period
        BASETIME = BASETIME - (BASETIME % 1000);
        DataStoreCache mmStore = getMetricMemoryStore2(BASETIME);

        QueryRequest query = new QueryRequest();
        query.setStart(BASETIME);
        query.setEnd(BASETIME + 1440000);
        query.setMsResolution(true);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setDownsample(Optional.of("1ms-avg"));
        subQuery.setMetric("metric.number.1");
        subQuery.addTag("host", ".*");
        QueryRequest.RateOption rateOption = new QueryRequest.RateOption();
        rateOption.setCounter(false);
        subQuery.setRate(true);
        subQuery.setRateOptions(rateOption);
        query.setQueries(Collections.singleton(subQuery));

        int x = 0;
        SortedKeyValueIterator<org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value> itr = null;
        try {
            // long firstTimestamp = Long.MAX_VALUE;
            long firstTimestamp = -1;
            long lastTimestamp = -1;
            int numSamples = 0;
            itr = mmStore.setupIterator(query, subQuery, new Authorizations(), Long.MAX_VALUE);
            while (itr.hasTop()) {
                itr.next();
                Map<Set<Tag>, Aggregation> aggregations = AggregationIterator.decodeValue(itr.getTopValue());
                for (Map.Entry<Set<Tag>, Aggregation> entry : aggregations.entrySet()) {
                    for (Sample s : entry.getValue()) {
                        numSamples++;
                        if (firstTimestamp == -1) {
                            firstTimestamp = s.timestamp;
                        }
                        lastTimestamp = s.timestamp;
                        // if (s.timestamp < firstTimestamp) {
                        // firstTimestamp = s.timestamp;
                        // }
                        // if (s.timestamp > lastTimestamp) {
                        // lastTimestamp = s.timestamp;
                        // }
                    }
                }
            }
            Assert.assertEquals("First timestamp incorrect", BASETIME + 1000, firstTimestamp);
            Assert.assertEquals("Last timestamp incorrect", BASETIME + 1440000, lastTimestamp);
            Assert.assertEquals("Number of samples incorrect", 2880, numSamples);
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("exception in test", e);
        }
    }

    private Metric createMetric(String metric, Map<String, String> tags, double value, long timestamp) {
        Metric m = new Metric();
        m.setName(metric);
        List<Tag> tagList = new ArrayList<>();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            tagList.add(new Tag(entry.getKey(), entry.getValue()));
        }
        m.setTags(tagList);
        Value metricValue = new Value();
        metricValue.setMeasure(value);
        metricValue.setTimestamp(timestamp);
        m.setValue(metricValue);
        return m;
    }

}

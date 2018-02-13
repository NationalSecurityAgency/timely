package timely.store.memory;

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
import timely.api.response.timeseries.QueryResponse;
import timely.model.Metric;
import timely.model.Tag;
import timely.model.Value;
import timely.sample.Aggregation;
import timely.sample.Sample;
import timely.sample.iterators.AggregationIterator;

import java.io.IOException;
import java.util.*;

public class TestMetricMemoryStoreIterator {

    private static final Logger LOG = LoggerFactory.getLogger(TestMetricMemoryStoreIterator.class);
    private static Configuration configuration = null;

    @BeforeClass
    public static void setup() {
        configuration = new Configuration();
        configuration.getSecurity().setAllowAnonymousAccess(true);
    }

    private MemoryDataStore getMetricMemoryStore1() throws TimelyException {

        MemoryDataStore mmStore = new MemoryDataStore(configuration);

        Map<String, String> tags = new HashMap<>();
        tags.put("part", "webservice");
        tags.put("instance", "sample");

        Random r = new Random();
        int value = 0;
        long timestamp = 1000;
        for (int x = 0; x <= 60 * 24; x++) {
            for (int y = 0; y < 2; y++) {
                tags.put("host", (y % 2 == 0) ? "r01n01" : "r02n01");
                tags.put("rack", (y % 2 == 0) ? "r01" : "r02");
                value = r.nextInt(1000);
                mmStore.store(createMetric("metric.number.1", tags, value, timestamp + (x * 1000)));
                mmStore.store(createMetric("metric.number.2", tags, value, timestamp + (x * 1000)));
                mmStore.store(createMetric("metric.number.3", tags, value, timestamp + (x * 1000)));
                mmStore.store(createMetric("metric.number.4", tags, value, timestamp + (x * 1000)));
                mmStore.store(createMetric("metric.number.5", tags, value, timestamp + (x * 1000)));
            }
        }
        return mmStore;
    }

    private MemoryDataStore getMetricMemoryStore2() throws TimelyException {

        MemoryDataStore mmStore = new MemoryDataStore(configuration);

        int increment = 10;
        Map<String, String> tags = new HashMap<>();
        tags.put("part", "webservice");
        tags.put("instance", "sample");

        Random r = new Random();
        int value = 0;
        // long timestamp = 1000;
        for (int x = 0; x <= 60 * 24; x++) {
            for (int y = 0; y < 2; y++) {
                tags.put("host", (y % 2 == 0) ? "r01n01" : "r02n01");
                tags.put("rack", (y % 2 == 0) ? "r01" : "r02");
                if (y % 2 == 0) {
                    value += increment;
                } else {
                    value += 2 * increment;
                }
                mmStore.store(createMetric("metric.number.1", tags, value, (x * 1000)));
                mmStore.store(createMetric("metric.number.2", tags, value, (x * 1000)));
                mmStore.store(createMetric("metric.number.3", tags, value, (x * 1000)));
                mmStore.store(createMetric("metric.number.4", tags, value, (x * 1000)));
                mmStore.store(createMetric("metric.number.5", tags, value, (x * 1000)));
            }
        }
        return mmStore;
    }

    @Test
    public void testDownsampleIterator() throws TimelyException {

        MemoryDataStore mmStore = getMetricMemoryStore1();

        QueryRequest query = new QueryRequest();
        query.setStart(0);
        query.setEnd(1440000);
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
            itr = mmStore.setupIterator(query, subQuery, new Authorizations());
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
                        // System.out.println(entry.toString() + " --> " +
                        // s.toString());
                    }
                }
            }
            Assert.assertEquals("First timestamp incorrect", 0, firstTimestamp);
            Assert.assertEquals("Last timestamp incorrect", 1440000, lastTimestamp);
            Assert.assertEquals("Number of samples incorrect", 50, numSamples);
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("exception in test", e);
        }
    }

    @Test
    public void testRateIterator() throws TimelyException {

        MemoryDataStore mmStore = getMetricMemoryStore2();

        QueryRequest query = new QueryRequest();
        query.setStart(0);
        query.setEnd(1440000);
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
            long firstTimestamp = -1;
            long lastTimestamp = -1;
            int numSamples = 0;
            itr = mmStore.setupIterator(query, subQuery, new Authorizations());
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
//                        if (x++ < 10) {
//                            System.out.println(entry.toString() + " --> " + s.toString());
//                        }
                    }
                }
            }
            Assert.assertEquals("First timestamp incorrect", 1000, firstTimestamp);
            Assert.assertEquals("Last timestamp incorrect", 1440000, lastTimestamp);
            Assert.assertEquals("Number of samples incorrect", 2879, numSamples);
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("exception in test", e);
        }
    }

    @Test
    public void testDownsampleIterator2() throws TimelyException {

        MemoryDataStore mmStore = getMetricMemoryStore1();

        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setStart(0);
        queryRequest.setEnd(1440000);
        queryRequest.setMsResolution(true);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setDownsample(Optional.of("1m-avg"));
        subQuery.setMetric("metric.number.1");
        subQuery.addTag("host", ".*");
        queryRequest.setQueries(Collections.singleton(subQuery));

        List<QueryResponse> response = mmStore.query(queryRequest);

//        for (QueryResponse r : response) {
//            System.out.println(r.getMetric() + " " + r.getTags() + " " + r.getDps().toString());
//        }
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

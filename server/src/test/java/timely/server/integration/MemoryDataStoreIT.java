package timely.server.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import timely.api.request.timeseries.QueryRequest;
import timely.api.response.timeseries.QueryResponse;
import timely.common.component.AuthenticationService;
import timely.common.configuration.CacheProperties;
import timely.model.Metric;
import timely.model.Tag;
import timely.model.Value;
import timely.server.store.InternalMetrics;
import timely.server.store.cache.DataStoreCache;
import timely.server.test.TestDataStoreCache;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class MemoryDataStoreIT extends ITBase {

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private CuratorFramework curatorFramework;

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private InternalMetrics internalMetrics;

    @Autowired
    private CacheProperties cacheProperties;

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    private DataStoreCache getMetricMemoryStore1(long baseTimestamp) throws Exception {

        DataStoreCache mmStore = new TestDataStoreCache(curatorFramework, authenticationService, internalMetrics, timelyProperties, cacheProperties);
        mmStore.start();

        long timestamp = baseTimestamp + 10000;
        Map<String,String> tags = new HashMap<>();

        tags.put("part", "webservice");
        tags.put("host", "host1");
        double[] values = new double[] {2.1, 2.3, 2.0, 2.8, 4.2, 2.1, 7.2, 4.1, 10.2};
        for (double d : values) {
            mmStore.store(createMetric("mymetric", tags, d, timestamp));
            timestamp += 1000;
        }

        tags.put("host", "host2");
        values = new double[] {2.9, 2.4, 2.2, 2.7, 4.2, 2.1, 7.2, 4.1, 10.2};
        for (double d : values) {
            mmStore.store(createMetric("mymetric", tags, d, timestamp));
            timestamp += 1000;
        }

        tags.put("part", "ingest");
        tags.put("host", "host3");
        values = new double[] {2.9, 2.4, 2.2, 2.7, 4.2, 2.1, 7.2, 4.1, 10.2};
        for (double d : values) {
            mmStore.store(createMetric("mymetric", tags, d, timestamp));
            timestamp += 1000;
        }
        mmStore.flushCaches(-1);
        return mmStore;
    }

    private DataStoreCache getMetricMemoryStore2(long baseTimestamp) throws Exception {

        DataStoreCache mmStore = new TestDataStoreCache(curatorFramework, authenticationService, internalMetrics, timelyProperties, cacheProperties);
        mmStore.start();

        Map<String,String> tags = new HashMap<>();
        tags.put("part", "webservice");
        tags.put("host", "r01n01");
        tags.put("rack", "r01n01");
        tags.put("instance", "sample");

        Random r = new Random();
        long timestamp = baseTimestamp;
        for (int x = 0; x <= 60 * 24; x++) {
            mmStore.store(createMetric("metric.number.1", tags, r.nextInt(1000), timestamp + (x * 1000)));
        }
        mmStore.flushCaches(-1);
        return mmStore;
    }

    @Test
    public void testFiveSecondDownsample() throws Exception {

        // 5 second boundary so that we know how many downsamples to expect
        long now = (System.currentTimeMillis() / 5000) * 5000;
        DataStoreCache mmStore = getMetricMemoryStore1(now);

        QueryRequest query = new QueryRequest();
        query.setStart(now);
        query.setEnd(now + 100000);
        query.setMsResolution(true);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setDownsample(Optional.of("5s-avg"));
        subQuery.setMetric("mymetric");
        subQuery.addTag("host", ".*");
        query.setQueries(Collections.singleton(subQuery));

        List<QueryResponse> responseList = mmStore.query(query);
        Assert.assertEquals(3, responseList.size());
        int downsamples = 0;
        for (QueryResponse r : responseList) {
            Assert.assertEquals("mymetric", r.getMetric());
            downsamples += r.getDps().size();
        }
        Assert.assertEquals(8, downsamples);
    }

    @Test
    public void testFiveMinuteDownsample() throws Exception {

        // 5 minute boundary so that we know how many downsamples to expect
        long now = (System.currentTimeMillis() / 300000) * 300000;
        DataStoreCache mmStore = getMetricMemoryStore2(now);

        QueryRequest query = new QueryRequest();
        query.setStart(now);
        query.setEnd(now + 86400000);
        query.setMsResolution(true);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setDownsample(Optional.of("5m-avg"));
        subQuery.setMetric("metric.number.1");
        subQuery.addTag("host", ".*");
        query.setQueries(Collections.singleton(subQuery));

        List<QueryResponse> responseList = mmStore.query(query);
        Assert.assertEquals(1, responseList.size());
        QueryResponse r = responseList.get(0);
        Assert.assertEquals("metric.number.1", r.getMetric());
        Assert.assertEquals(5, r.getDps().size());
    }

    private Metric createMetric(String metric, Map<String,String> tags, double value, long timestamp) {
        Metric m = new Metric();
        m.setName(metric);
        List<Tag> tagList = new ArrayList<>();
        for (Map.Entry<String,String> entry : tags.entrySet()) {
            tagList.add(new Tag(entry.getKey(), entry.getValue()));
        }
        m.setTags(tagList);
        Value metricValue = new Value();
        metricValue.setMeasure(value);
        metricValue.setTimestamp(timestamp);
        m.setValue(metricValue);
        return m;
    }

    @Test
    public void TestExtentOfStorage() throws Exception {
        DataStoreCache mmStore = new TestDataStoreCache(curatorFramework, authenticationService, internalMetrics, timelyProperties, cacheProperties);
        mmStore.start();

        HashMap<String,String> tags = new HashMap<>();
        tags.put("host", "localhost");

        long now = System.currentTimeMillis();
        long timestamp = now;

        for (int x = 1; x <= 100; x++) {

            Metric m = createMetric("test.metric", tags, 2.0, timestamp);
            mmStore.store(m);
            mmStore.flushCaches(-1);
            timestamp = timestamp + 60000;

            QueryRequest query = new QueryRequest();
            query.setStart(now);
            query.setEnd(now + 86400000);
            query.setMsResolution(true);
            QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
            subQuery.setMetric("test.metric");
            query.setQueries(Collections.singleton(subQuery));

            List<QueryResponse> responseList = mmStore.query(query);
            long totalObservations = 0;
            for (QueryResponse r : responseList) {
                totalObservations += r.getDps().size();
            }
            Assert.assertEquals("Unexpected number of total observations", x, totalObservations);
        }
    }
}

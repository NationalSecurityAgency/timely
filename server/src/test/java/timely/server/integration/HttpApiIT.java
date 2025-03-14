package timely.server.integration;

import static java.nio.charset.StandardCharsets.UTF_8;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.collections.CollectionUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import timely.api.request.VersionRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.common.configuration.HttpProperties;
import timely.model.Metric;
import timely.model.Tag;
import timely.model.Value;
import timely.server.test.TestConfiguration;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;
import timely.util.JsonUtil;

/**
 * Integration tests for the operations available over the HTTP transport
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"oneWaySsl"})
public class HttpApiIT extends OneWaySSLBase {

    private static final Long TEST_TIME = ITBase.roundTimestampToLastHour(System.currentTimeMillis());

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private HttpProperties httpProperties;

    private String baseUrl;

    @Before
    public void setup() {
        super.setup();
        baseUrl = "https://" + httpProperties.getHost() + ":" + httpProperties.getPort();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testSuggest() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
           "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
           "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

        String suggest = baseUrl + "/api/suggest?";
        // Test prefix matching
        String result = query(suggest + "type=metrics&m=sys&max=10");
        assertEquals("[\"sys.cpu.idle\",\"sys.cpu.user\"]", result);

        // Test max
        result = query(suggest + "type=metrics&m=sys&max=1");
        assertEquals("[\"sys.cpu.idle\"]", result);

        // Test empty query
        result = query(suggest + "type=metrics&max=10");
        assertEquals("[\"sys.cpu.idle\",\"sys.cpu.user\"]", result);
    }

    @Test
    public void testMetrics() throws Exception {
      // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
            "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
            "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 viz=(a|b|c)",
            "zzzz 1234567892 1.0 host=localhost");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

        String metrics = baseUrl + "/api/metrics";
        // Test prefix matching
        String result = query(metrics);
        Document doc = Jsoup.parse(result);
        Elements tableData = doc.select("td");

        assertEquals(1, tableData.select(":contains(sys.cpu.user)").size());
        assertEquals(1, tableData.select(":contains(tag1=value1 tag2=value2)").size());
        assertEquals(1, tableData.select(":contains(sys.cpu.idle)").size());
        assertEquals(1, tableData.select(":contains(tag3=value3 tag4=value4)").size());
        assertEquals(1, tableData.select(":contains(zzzz)").size());
        assertEquals(1, tableData.select(":contains(host=localhost)").size());
    }

    @Test
    public void testMetricsJson() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 viz=(a|b|c)",
                "zzzz 1234567892 1.0 host=localhost");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

        String metrics = baseUrl + "/api/metrics";
        // Test prefix matching
        String result = query(metrics, "application/json");

        Gson gson = new Gson();
        JsonObject response = gson.fromJson(result, JsonObject.class);
        JsonArray metricsArray = response.getAsJsonArray("metrics");
        assertEquals(3, metricsArray.size());

        for (JsonElement e : metricsArray) {
            JsonObject metricObject = e.getAsJsonObject();
            String metricName = metricObject.get("metric").getAsString();
            Iterator<JsonElement> tagItr = metricObject.get("tags").getAsJsonArray().iterator();
            Multimap<String,String> tagMap = HashMultimap.create();
            while (tagItr.hasNext()) {
                JsonObject tagObject = tagItr.next().getAsJsonObject();
                tagMap.put(tagObject.get("key").getAsString(), tagObject.get("value").getAsString());
            }
            switch (metricName) {
                case "sys.cpu.user":
                    assertEquals(2, tagMap.size());
                    assertTrue(CollectionUtils.isEqualCollection(Arrays.asList("value1"), tagMap.get("tag1")));
                    assertTrue(CollectionUtils.isEqualCollection(Arrays.asList("value2"), tagMap.get("tag2")));
                    break;
                case "sys.cpu.idle":
                    assertEquals(2, tagMap.size());
                    assertTrue(CollectionUtils.isEqualCollection(Arrays.asList("value3"), tagMap.get("tag3")));
                    assertTrue(CollectionUtils.isEqualCollection(Arrays.asList("value4"), tagMap.get("tag4")));
                    break;
                case "zzzz":
                    assertEquals(1, tagMap.size());
                    assertTrue(CollectionUtils.isEqualCollection(Arrays.asList("localhost"), tagMap.get("host")));
                    break;
            }
        }
    }

    @Test
    public void testLookup() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
           "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3",
           "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
           "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

        String suggest = baseUrl + "/api/search/lookup?";

        // Test a known query
        String result = query(suggest + "m=sys.cpu.idle%7Btag3%3D.*%7D");
        assertTrue(result, result.indexOf("\"results\":[{\"tags\":{\"tag3\":\"value3\"}") >= 0);

        // Test a fail
        result = query(suggest + "m=sys.cpu.idle%7Btag3%3Dnomatch%7D");
        assertTrue(result.indexOf("\"results\":[]") >= 0);

        // Test multiple results
        result = query(suggest + "m=sys.cpu.idle%7Btag3%3D.*,tag4%3D.*%7D");
        assertTrue(result, result.indexOf("\"results\":[{\"tags\":{\"tag3\":\"value3\"}") >= 0);
        assertTrue(result, result.indexOf("{\"tags\":{\"tag4\":\"value4\"}") >= 0);

        // Find a tag only in the metric that matches
        result = query(suggest + "m=sys.cpu.idle%7Btag1%3D.*%7D");
        assertTrue(result, result.indexOf("\"results\":[]") >= 0);
    }

    @Test
    public void testQueryWithMsResolution() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
           "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3",
           "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
           "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        request.setMsResolution(true);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        subQuery.setTags(Collections.singletonMap("tag3", "value3"));
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        Map<String,String> tags = response.get(0).getTags();
        assertEquals(1, tags.size());
        assertTrue(tags.containsKey("tag3"));
        assertTrue(tags.get("tag3").equals("value3"));
        Map<String,Object> dps = response.get(0).getDps();
        assertEquals(2, dps.size());
        Iterator<Entry<String,Object>> entries = dps.entrySet().iterator();
        Entry<String,Object> entry = entries.next();
        // We are downsampling to 1 second by default, which is why the
        // times in the results end with ms of the start parameter.
        assertEquals(getBaselineStart(TEST_TIME), entry.getKey());
        assertEquals(1.0, entry.getValue());
        entry = entries.next();
        assertEquals(getBaselineStart(TEST_TIME + 1000), entry.getKey());
        assertEquals(3.0, entry.getValue());
    }

    private String getBaselineStart(Long time) {
        String timeString = time.toString();
        return timeString.substring(0, timeString.length() - 3) + "000";

    }

    @Test
    public void testQueryWithoutMsResolution() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
           "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3",
           "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
           "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        subQuery.setTags(Collections.singletonMap("tag3", "value3"));
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        Map<String,String> tags = response.get(0).getTags();
        assertEquals(1, tags.size());
        assertTrue(tags.containsKey("tag3"));
        assertTrue(tags.get("tag3").equals("value3"));
        Map<String,Object> dps = response.get(0).getDps();
        assertEquals(2, dps.size());
        Iterator<Entry<String,Object>> entries = dps.entrySet().iterator();
        Entry<String,Object> entry = entries.next();
        assertEquals(Long.toString((TEST_TIME / 1000)), entry.getKey());
        assertEquals(1.0, entry.getValue());
        entry = entries.next();
        assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
        assertEquals(3.0, entry.getValue());
    }

    @Test
    public void testQueryWithNoTags() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
           "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3",
           "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
           "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        Map<String,String> tags = response.get(0).getTags();
        assertEquals(0, tags.size());
        Map<String,Object> dps = response.get(0).getDps();
        assertEquals(2, dps.size());
        Iterator<Entry<String,Object>> entries = dps.entrySet().iterator();
        Entry<String,Object> entry = entries.next();
        assertEquals(Long.toString((TEST_TIME / 1000)), entry.getKey());
        assertEquals(1.0, entry.getValue());
        entry = entries.next();
        assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
        assertEquals(3.0, entry.getValue());
    }

    @Test
    public void testQueryWithNoTagsMultipleSeries() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 host=h1",
           "sys.cpu.user " + TEST_TIME + " 2.0 tag1=value1 tag2=value2 host=h2",
           "sys.cpu.user " + (TEST_TIME + 1000) + " 4.0 tag1=value1 tag2=value2 host=h1",
           "sys.cpu.user " + (TEST_TIME + 1000) + " 3.0 tag1=value1 tag2=value2 host=h2",
           "sys.cpu.user " + (TEST_TIME + 2000) + " 5.0 tag1=value1 tag2=value2 host=h1",
           "sys.cpu.user " + (TEST_TIME + 2000) + " 6.0 tag1=value1 tag2=value2 host=h2");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 4000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.user");
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        Map<String,String> tags = response.get(0).getTags();
        assertEquals(0, tags.size());
        Map<String,Object> dps = response.get(0).getDps();
        assertEquals(3, dps.size());
        Iterator<Entry<String,Object>> entries = dps.entrySet().iterator();
        Entry<String,Object> entry = entries.next();
        assertEquals(Long.toString((TEST_TIME / 1000)), entry.getKey());
        assertEquals(2.0, entry.getValue());
        entry = entries.next();
        assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
        assertEquals(4.0, entry.getValue());
        entry = entries.next();
        assertEquals(Long.toString((TEST_TIME / 1000) + 2), entry.getKey());
        assertEquals(6.0, entry.getValue());
    }

    @Test(expected = NotSuccessfulException.class)
    public void testQueryWithNoMatchingTags() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
           "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3 rack=r2",
           "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 rack=r1",
           "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4 rack=r2");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        subQuery.setTags(Collections.singletonMap("rack", "r3"));
        request.addQuery(subQuery);
        query(baseUrl + "/api/query", request, 400);
    }

    @Test
    public void testQueryWithTagWildcard() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
            "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3 rack=r2",
            "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 rack=r1",
            "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4 rack=r2");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        subQuery.setTags(Collections.singletonMap("rack", "r.*"));
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);

        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(2, response.size());

        AtomicInteger rack1Count = new AtomicInteger(0);
        AtomicInteger rack2Count = new AtomicInteger(0);
        response.forEach(r -> {
            Map<String,String> tags = r.getTags();
            Map<String,Object> dps = r.getDps();
            assertEquals(1, tags.size());
            assertEquals(1, dps.size());
            assertTrue(tags.containsKey("rack"));
            Value value = parseDps(dps);
            switch (tags.get("rack")) {
                case "r2":
                    assertEquals((Long) ((TEST_TIME / 1000L) + 1L), value.getTimestamp());
                    assertEquals(3.0D, value.getMeasure(), 0.0);
                    rack2Count.incrementAndGet();
                    break;
                case "r1":
                    assertEquals((Long) (TEST_TIME / 1000L), value.getTimestamp());
                    assertEquals(1.0D, value.getMeasure(), 0.0);
                    rack1Count.incrementAndGet();
                    break;
                default:
                    assertTrue("Found invalid rack number: " + tags.get("rack"), false);
                    break;
            }

        });
        assertEquals("Did not find rack=r1", 1, rack1Count.get());
        assertEquals("Did not find rack=r2", 1, rack2Count.get());
    }

    private Value parseDps(Map<String,Object> dps) {
        Iterator<Entry<String,Object>> entries = dps.entrySet().iterator();
        Entry<String,Object> entry = entries.next();
        return new Value(Long.parseLong(entry.getKey()), (Double) entry.getValue());
    }

    @Test
    public void testQueryWithTagWildcard2() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
            "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3 rack=r2",
            "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 rack=r1",
            "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4 rack=r2");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        subQuery.setTags(Collections.singletonMap("rack", ".*"));
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);

        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(2, response.size());

        AtomicInteger rack1Count = new AtomicInteger(0);
        AtomicInteger rack2Count = new AtomicInteger(0);
        response.forEach(r -> {
            Map<String,String> tags = r.getTags();
            Map<String,Object> dps = r.getDps();
            assertEquals(1, tags.size());
            assertEquals(1, dps.size());
            assertTrue(tags.containsKey("rack"));
            Value value = parseDps(dps);
            switch (tags.get("rack")) {
                case "r2":
                    assertEquals((Long) ((TEST_TIME / 1000L) + 1L), value.getTimestamp());
                    assertEquals(3.0D, value.getMeasure(), 0.0);
                    rack2Count.incrementAndGet();
                    break;
                case "r1":
                    assertEquals((Long) (TEST_TIME / 1000L), value.getTimestamp());
                    assertEquals(1.0D, value.getMeasure(), 0.0);
                    rack1Count.incrementAndGet();
                    break;
                default:
                    assertTrue("Found invalid rack number: " + tags.get("rack"), false);
                    break;
            }

        });
        assertEquals("Did not find rack=r1", 1, rack1Count.get());
        assertEquals("Did not find rack=r2", 1, rack2Count.get());
    }

    @Test
    public void testQueryWithTagOr() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
            "sys.cpu.user " + (TEST_TIME + 1L) + " 1.0 tag3=value3 rack=r2",
            "sys.cpu.idle " + (TEST_TIME + 2L) + " 2.0 tag3=value3 tag4=value4 rack=r1",
            "sys.cpu.idle " + (TEST_TIME + 1000L) + " 3.0 tag3=value3 tag4=value4 rack=r2",
            "sys.cpu.idle " + (TEST_TIME + 2000L) + " 4.0 tag3=value3 tag4=value4 rack=r3");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();

        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        request.setMsResolution(true);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        subQuery.setTags(Collections.singletonMap("rack", "r1|r2"));
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);
        List<QueryResponse> responses = query(baseUrl + "/api/query", request);

        assertEquals("Expected 2 responses for sys.cpu.idle rack=r1|r2", 2, responses.size());

        AtomicInteger rack1Count = new AtomicInteger(0);
        AtomicInteger rack2Count = new AtomicInteger(0);
        for (QueryResponse response : responses) {
            assertEquals("Found incorrect metric", "sys.cpu.idle", response.getMetric());
            response.getTags().forEach((tagk, tagv) -> {
                switch (tagk) {
                    case "rack":
                        assertTrue(tagv.equals("r1") || tagv.equals("r2"));
                        Map<String,Object> dps = response.getDps();
                        assertEquals(1, dps.size());
                        Entry<String,Object> dpsEntry = dps.entrySet().iterator().next();
                        Long ts = Long.parseLong(dpsEntry.getKey());
                        double measure = (Double) dpsEntry.getValue();
                        switch (tagv) {
                            case "r1":
                                assertEquals(2.0D, measure, 0.0);
                                assertEquals("TEST_TIME: " + getBaselineStart(TEST_TIME + 2), getBaselineStart(TEST_TIME + 2), ts.toString());
                                rack1Count.getAndIncrement();
                                break;
                            case "r2":
                                assertEquals(3.0D, measure, 0.0);
                                assertEquals("TEST_TIME: " + getBaselineStart(TEST_TIME + 1000), getBaselineStart(TEST_TIME + 1000), ts.toString());
                                rack2Count.getAndIncrement();
                                break;
                            default:
                                throw new IllegalArgumentException("Found incorrect rack tag value: " + tagv);
                        }
                        break;
                    case "tag1":
                        assertTrue("Found incorrect tag in results", false);
                        break;
                    case "tag2":
                        assertTrue("Found incorrect tag in results", false);
                        break;
                    case "tag3":
                        assertTrue("Found incorrect tag in results", tagv.equals("tag3"));
                        break;
                    case "tag4":
                        assertTrue("Found incorrect tag in results", tagv.equals("tag4"));
                        break;
                    default:
                        throw new IllegalArgumentException("Found incorrect tag key: " + tagk);
                }

            });
        }
        assertEquals("Did not find rack=r1", 1, rack1Count.get());
        assertEquals("Did not find rack=r2", 1, rack2Count.get());
    }

    @Test
    public void testQueryWithTagRegex1() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
           "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3 rack=r2",
           "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 rack=r1",
           "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4 rack=r2");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        Map<String,String> t = new LinkedHashMap<>();
        t.put("rack", "r1|r2");
        t.put("tag3", "value3");
        subQuery.setTags(t);
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(2, response.size());
        for (QueryResponse r : response) {
            Assert.assertEquals(2, r.getTags().size());
            Assert.assertTrue(r.getTags().containsKey("rack"));
            Assert.assertTrue(r.getTags().get("rack").matches("r1|r2"));
            Assert.assertTrue(r.getTags().containsKey("tag3"));
            Assert.assertTrue(r.getTags().get("tag3").equals("value3"));
            Map<String,Object> dps = r.getDps();
            assertEquals(1, dps.size());
            Object o = dps.entrySet().stream().map(e -> e.getValue()).findFirst().orElse(null);
            Assert.assertNotNull(o);
            switch (r.getTags().get("rack")) {
                case "r1":
                    assertEquals(1.0, o);
                    break;
                case "r2":
                    assertEquals(3.0, o);
                    break;
            }
        }
    }

    @Test
    public void testQueryWithTagRegex2() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
           "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3 rack=r2",
           "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 rack=r1",
           "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4 rack=r2");
        // @formatter:on
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.idle");
        Map<String,String> t = new LinkedHashMap<>();
        t.put("rack", "r1|r2");
        t.put("tag3", "val.*");
        subQuery.setTags(t);
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(2, response.size());
        for (QueryResponse r : response) {
            Assert.assertEquals(2, r.getTags().size());
            Assert.assertTrue(r.getTags().containsKey("rack"));
            Assert.assertTrue(r.getTags().get("rack").matches("r1|r2"));
            Assert.assertTrue(r.getTags().containsKey("tag3"));
            Assert.assertTrue(r.getTags().get("tag3").equals("value3"));
            Map<String,Object> dps = r.getDps();
            assertEquals(1, dps.size());
            Object o = dps.entrySet().stream().map(e -> e.getValue()).findFirst().orElse(null);
            Assert.assertNotNull(o);
            switch (r.getTags().get("rack")) {
                case "r1":
                    assertEquals(1.0, o);
                    break;
                case "r2":
                    assertEquals(3.0, o);
                    break;
            }
        }
    }

    private void addRateData(String metric, String tagString, long testStartTime, long timeIncrement, long testStopTime, double startValue, double maxValue,
                    double valueIncrement) throws Exception {
        long ts = testStartTime;
        double value = startValue;
        while (ts <= testStopTime) {
            put(metric + " " + ts + " " + value + " " + tagString);
            ts += timeIncrement;
            value = (value >= maxValue) ? startValue : (value + valueIncrement);
        }
    }

    @Test
    public void testRateQuery() throws Exception {
        addRateData("sys.cpu.user", "tag1=value1 tag2=value2 rack=r1", TEST_TIME, 1, TEST_TIME + 14, 1.0, Double.MAX_VALUE, 1.0);
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 15);
        request.setMsResolution(true);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.user");
        Map<String,String> t = new LinkedHashMap<>();
        subQuery.setTags(t);
        subQuery.setDownsample(Optional.of("1ms-max"));
        subQuery.setRate(true);
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        QueryResponse response1 = response.get(0);
        Map<String,Object> dps = response1.getDps();
        assertEquals(14, dps.size());
        int i = 1;
        for (Entry<String,Object> e : dps.entrySet()) {
            assertEquals(Long.toString(TEST_TIME + i), e.getKey());
            assertEquals(1.0, e.getValue());
            i++;
        }
    }

    @Test
    public void testRateCounterQuery() throws Exception {
        addRateData("sys.cpu.user", "tag1=value1 tag2=value2 rack=r1", TEST_TIME, 1, TEST_TIME + 14, 1.0, 10.0, 1.0);
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 15);
        request.setMsResolution(true);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.user");
        Map<String,String> t = new LinkedHashMap<>();
        subQuery.setTags(t);
        subQuery.setDownsample(Optional.of("1ms-max"));
        subQuery.setRate(true);
        subQuery.getRateOptions().setCounter(true);
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        QueryResponse response1 = response.get(0);
        Map<String,Object> dps = response1.getDps();
        assertEquals(14, dps.size());
        int i = 1;
        for (Entry<String,Object> e : dps.entrySet()) {
            assertEquals(Long.toString(TEST_TIME + i), e.getKey());
            assertEquals(1.0, e.getValue());
            i++;
        }
    }

    @Test
    public void testRateCounterWithCounterMaxOption() throws Exception {
        addRateData("sys.cpu.user", "tag1=value1 tag2=value2 rack=r1", TEST_TIME, 1, TEST_TIME + 14, 1.0, 10.0, 1.0);
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 15);
        request.setMsResolution(true);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.user");
        Map<String,String> t = new LinkedHashMap<>();
        subQuery.setTags(t);
        subQuery.setDownsample(Optional.of("1ms-max"));
        subQuery.setRate(true);
        subQuery.getRateOptions().setCounter(true);
        subQuery.getRateOptions().setCounterMax(10);
        subQuery.getRateOptions().setResetValue(2);
        request.addQuery(subQuery);
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        QueryResponse response1 = response.get(0);
        Map<String,Object> dps = response1.getDps();
        assertEquals(14, dps.size());
        int i = 1;
        for (Entry<String,Object> e : dps.entrySet()) {
            assertEquals(Long.toString(TEST_TIME + i), e.getKey());
            assertEquals(1.0, e.getValue());
            i++;
        }
    }

    @Test
    public void testRateCounterWithRateIntervalOption() throws Exception {
        addRateData("sys.cpu.user", "tag1=value1 tag2=value2 rack=r1", TEST_TIME, 60000, TEST_TIME + 600000, 1.0, Double.MAX_VALUE, 5);
        dataStore.flush();
        dataStoreCache.flushCaches(-1);
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 600000);
        request.setMsResolution(true);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.user");
        subQuery.setDownsample(Optional.of("1m-max"));
        subQuery.setRate(true);
        subQuery.getRateOptions().setInterval("2m");
        request.addQuery(subQuery);

        // test interval (2m) that is different from the downsample period (1m)
        List<QueryResponse> response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        QueryResponse response1 = response.get(0);
        Map<String,Object> dps = response1.getDps();
        assertEquals(10, dps.size());
        int i = 1;
        for (Entry<String,Object> e : dps.entrySet()) {
            assertEquals(Long.toString(TEST_TIME + (60000 * i)), e.getKey());
            assertEquals(10.0, e.getValue());
            i++;
        }

        // test that interval defaults to downsample period
        subQuery.getRateOptions().setInterval(null);
        response = query(baseUrl + "/api/query", request);
        assertEquals(1, response.size());
        response1 = response.get(0);
        dps = response1.getDps();
        assertEquals(10, dps.size());
        i = 1;
        for (Entry<String,Object> e : dps.entrySet()) {
            assertEquals(Long.toString(TEST_TIME + (60000 * i)), e.getKey());
            assertEquals(5.0, e.getValue());
            i++;
        }
    }

    @Test
    public void testGetVersion() throws Exception {
        String response = query(baseUrl + "/version", "application/json");
        assertNotNull(response);
        assertEquals(VersionRequest.VERSION, response);
    }

    @Test
    public void testPutMetric() throws Exception {
        Metric m = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D).tag(new Tag("tag1", "value1")).build();
        new Metric();
        URL url = new URL(baseUrl + "/api/put");
        HttpsURLConnection con = getUrlConnection(url);
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        String requestJSON = JsonUtil.getObjectMapper().writeValueAsString(m);
        con.setRequestProperty("Content-Length", String.valueOf(requestJSON.length()));
        OutputStream wr = con.getOutputStream();
        wr.write(requestJSON.getBytes(UTF_8));
        int responseCode = con.getResponseCode();
        Assert.assertEquals(200, responseCode);
    }
}

package timely.test.integration.http;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import timely.Server;
import timely.model.Metric;
import timely.model.Tag;
import timely.api.request.VersionRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.model.Value;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import timely.test.integration.OneWaySSLBase;
import timely.util.JsonUtil;

/**
 * Integration tests for the operations available over the HTTP transport
 */
@Category(IntegrationTest.class)
public class HttpApiIT extends OneWaySSLBase {

    private static final Long TEST_TIME = System.currentTimeMillis();

    @Test
    public void testSuggest() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4", "zzzz 1234567892 1.0 host=localhost");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            String suggest = "https://localhost:54322/api/suggest?";
            // Test prefix matching
            String result = query(suggest + "type=metrics&q=sys&max=10");
            assertEquals("[\"sys.cpu.idle\",\"sys.cpu.user\"]", result);

            // Test max
            result = query(suggest + "type=metrics&q=sys&max=1");
            assertEquals("[\"sys.cpu.idle\"]", result);

            // Test empty query
            result = query(suggest + "type=metrics&max=10");
            assertEquals("[\"sys.cpu.idle\",\"sys.cpu.user\",\"zzzz\"]", result);

            // Test skipping over initial metrics
            result = query(suggest + "type=metrics&q=z&max=10");
            assertEquals("[\"zzzz\"]", result);
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testMetrics() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            // @formatter:off
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 viz=(a|b|c)",
                "zzzz 1234567892 1.0 host=localhost");

            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            String metrics = "https://localhost:54322/api/metrics";
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
            // @formatter:on
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testMetricsJson() throws Exception {
        // @formatter:off

        String expected = "{\"metrics\":[{\"metric\":\"sys.cpu.user\",\"tags\":[{\"key\":\"tag2\",\"value\":\"value2\"},{\"key\":\"tag1\",\"value\":\"value1\"}]},{\"metric\":\"sys.cpu.idle\",\"tags\":[{\"key\":\"tag4\",\"value\":\"value4\"},{\"key\":\"tag3\",\"value\":\"value3\"}]},{\"metric\":\"zzzz\",\"tags\":[{\"key\":\"host\",\"value\":\"localhost\"}]}]}";
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                    "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 viz=(a|b|c)",
                    "zzzz 1234567892 1.0 host=localhost");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            String metrics = "https://localhost:54322/api/metrics";
            // Test prefix matching
            String result = query(metrics, "application/json");
            assertEquals(expected, result);
        } finally {
            s.shutdown();
        }
        // @formatter:on

    }

    @Test
    public void testLookup() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            String suggest = "https://localhost:54322/api/search/lookup?";

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

        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithMsResolution() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
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
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(1, tags.size());
            assertTrue(tags.containsKey("tag3"));
            assertTrue(tags.get("tag3").equals("value3"));
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(2, dps.size());
            Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
            Entry<String, Object> entry = entries.next();
            // We are downsampling to 1 second by default, which is why the
            // times in the results end with ms of the start parameter.
            assertEquals(TEST_TIME.toString(), entry.getKey());
            assertEquals(1.0, entry.getValue());
            entry = entries.next();
            assertEquals(Long.toString(TEST_TIME + 1000), entry.getKey());
            assertEquals(3.0, entry.getValue());
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithoutMsResolution() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
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
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(1, tags.size());
            assertTrue(tags.containsKey("tag3"));
            assertTrue(tags.get("tag3").equals("value3"));
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(2, dps.size());
            Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
            Entry<String, Object> entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000)), entry.getKey());
            assertEquals(1.0, entry.getValue());
            entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
            assertEquals(3.0, entry.getValue());
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithNoTags() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(0, tags.size());
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(2, dps.size());
            Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
            Entry<String, Object> entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000)), entry.getKey());
            assertEquals(1.0, entry.getValue());
            entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
            assertEquals(3.0, entry.getValue());
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithNoTagsMultipleSeries() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 host=h1", "sys.cpu.user " + TEST_TIME
                    + " 2.0 tag1=value1 tag2=value2 host=h2", "sys.cpu.user " + (TEST_TIME + 1000)
                    + " 4.0 tag1=value1 tag2=value2 host=h1", "sys.cpu.user " + (TEST_TIME + 1000)
                    + " 3.0 tag1=value1 tag2=value2 host=h2", "sys.cpu.user " + (TEST_TIME + 2000)
                    + " 5.0 tag1=value1 tag2=value2 host=h1", "sys.cpu.user " + (TEST_TIME + 2000)
                    + " 6.0 tag1=value1 tag2=value2 host=h1");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 4000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.user");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(0, tags.size());
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(3, dps.size());
            Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
            Entry<String, Object> entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000)), entry.getKey());
            assertEquals(2.0, entry.getValue());
            entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
            assertEquals(4.0, entry.getValue());
            entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000) + 2), entry.getKey());
            assertEquals(6.0, entry.getValue());
        } finally {
            s.shutdown();
        }
    }

    @Test(expected = NotSuccessfulException.class)
    public void testQueryWithNoMatchingTags() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            subQuery.setTags(Collections.singletonMap("rack", "r3"));
            request.addQuery(subQuery);
            query("https://127.0.0.1:54322/api/query", request, 400);
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithTagWildcard() throws Exception {
        final Server s = new Server(conf);
        s.run();
        // @formatter:off
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
                    "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3 rack=r2",
                    "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 rack=r1",
                    "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4 rack=r2");
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

            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(2, response.size());

            AtomicInteger rack1Count = new AtomicInteger(0);
            AtomicInteger rack2Count = new AtomicInteger(0);
            response.forEach(r -> {
                Map<String, String> tags = r.getTags();
                Map<String, Object> dps = r.getDps();
                assertEquals(1, tags.size());
                assertEquals(1, dps.size());
                assertTrue(tags.containsKey("rack"));
                Value value = parseDps(dps);
                switch (tags.get("rack")) {
                    case "r2":
                        assertEquals((Long)((TEST_TIME / 1000L) + 1L), value.getTimestamp());
                        assertEquals(3.0D, value.getMeasure(), 0.0);
                        rack2Count.incrementAndGet();
                        break;
                    case "r1":
                        assertEquals((Long)(TEST_TIME / 1000L ), value.getTimestamp());
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
            // @formatter:on

        } finally {
            s.shutdown();
        }
    }

    private Value parseDps(Map<String, Object> dps) {
        Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
        Entry<String, Object> entry = entries.next();
        return new Value(Long.parseLong(entry.getKey()), (Double) entry.getValue());
    }

    @Test
    public void testQueryWithTagWildcard2() throws Exception {
        final Server s = new Server(conf);
        s.run();
        // @formatter:off
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
                    "sys.cpu.user " + (TEST_TIME + 1) + " 1.0 tag3=value3 rack=r2",
                    "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 rack=r1",
                    "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4 rack=r2");
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

            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(2, response.size());

            AtomicInteger rack1Count = new AtomicInteger(0);
            AtomicInteger rack2Count = new AtomicInteger(0);
            response.forEach(r -> {
                Map<String, String> tags = r.getTags();
                Map<String, Object> dps = r.getDps();
                assertEquals(1, tags.size());
                assertEquals(1, dps.size());
                assertTrue(tags.containsKey("rack"));
                Value value = parseDps(dps);
                switch (tags.get("rack")) {
                    case "r2":
                        assertEquals((Long)((TEST_TIME / 1000L) + 1L), value.getTimestamp());
                        assertEquals(3.0D, value.getMeasure(), 0.0);
                        rack2Count.incrementAndGet();
                        break;
                    case "r1":
                        assertEquals((Long)(TEST_TIME / 1000L ), value.getTimestamp());
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

        } finally {
            s.shutdown();
        }
        // @formatter:on

    }

    @Test
    public void testQueryWithTagOr() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            // @formatter:off
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
                    "sys.cpu.user " + (TEST_TIME + 1L) + " 1.0 tag3=value3 rack=r2",
                    "sys.cpu.idle " + (TEST_TIME + 2L) + " 2.0 tag3=value3 tag4=value4 rack=r1",
                    "sys.cpu.idle " + (TEST_TIME + 1000L) + " 3.0 tag3=value3 tag4=value4 rack=r2",
                    "sys.cpu.idle " + (TEST_TIME + 2000L) + " 4.0 tag3=value3 tag4=value4 rack=r3");
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
            List<QueryResponse> responses = query("https://127.0.0.1:54322/api/query", request);

            assertEquals("Expected 2 responses for sys.cpu.idle rack=r1|r2", 2, responses.size());

            AtomicInteger rack1Count = new AtomicInteger(0);
            AtomicInteger rack2Count = new AtomicInteger(0);
            for(QueryResponse response: responses){
                assertTrue("Found incorrect metric", "sys.cpu.idle".equals(response.getMetric()));
                response.getTags().forEach((tagk, tagv) -> {
                   switch (tagk){
                       case "rack":
                           assertTrue(tagv.equals("r1") || tagv.equals("r2"));
                           Map<String, Object> dps = response.getDps();
                           assertEquals(1,dps.size());
                           Entry<String, Object> dpsEntry = dps.entrySet().iterator().next();
                           long ts = Long.parseLong(dpsEntry.getKey());
                           double measure = (Double)dpsEntry.getValue();
                           switch(tagv){
                               case "r1":
                                   assertEquals(2.0D, measure, 0.0);
                                   // TODO re-evaluate this when #81 is resolved
                                   // assertEquals("TEST_TIME: " + TEST_TIME, TEST_TIME + 2L, ts);
                                   rack1Count.getAndIncrement();
                                   break;
                               case "r2":
                                   assertEquals(3.0D, measure, 0.0);
                                   assertEquals("TEST_TIME: " + TEST_TIME, TEST_TIME + 1000L, ts);
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

            // @formatter:on
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithTagRegex() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            Map<String, String> t = new LinkedHashMap<>();
            t.put("rack", "r1|r2");
            t.put("tag3", "value3");
            subQuery.setTags(t);
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(2, response.size());
            QueryResponse response1 = response.get(0);
            Map<String, String> tags = response1.getTags();
            assertEquals(2, tags.size());
            assertTrue(tags.containsKey("rack"));
            assertTrue(tags.get("rack").equals("r2"));
            assertTrue(tags.containsKey("tag3"));
            assertTrue(tags.get("tag3").equals("value3"));
            Map<String, Object> dps = response1.getDps();
            assertEquals(1, dps.size());
            Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
            Entry<String, Object> entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
            assertEquals(3.0, entry.getValue());

            QueryResponse response2 = response.get(1);
            Map<String, String> tags2 = response2.getTags();
            assertEquals(2, tags2.size());
            assertTrue(tags2.containsKey("rack"));
            assertTrue(tags2.get("rack").equals("r1"));
            assertTrue(tags.containsKey("tag3"));
            assertTrue(tags.get("tag3").equals("value3"));
            Map<String, Object> dps2 = response2.getDps();
            assertEquals(1, dps2.size());
            Iterator<Entry<String, Object>> entries2 = dps2.entrySet().iterator();
            Entry<String, Object> entry2 = entries2.next();
            assertEquals(Long.toString((TEST_TIME / 1000)), entry2.getKey());
            assertEquals(1.0, entry2.getValue());
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithTagRegex2() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            Map<String, String> t = new LinkedHashMap<>();
            t.put("rack", "r1|r2");
            t.put("tag3", "val.*");
            subQuery.setTags(t);
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(2, response.size());
            QueryResponse response1 = response.get(0);
            Map<String, String> tags = response1.getTags();
            assertEquals(2, tags.size());
            assertTrue(tags.containsKey("rack"));
            assertTrue(tags.get("rack").equals("r2"));
            assertTrue(tags.containsKey("tag3"));
            assertTrue(tags.get("tag3").equals("value3"));
            Map<String, Object> dps = response1.getDps();
            assertEquals(1, dps.size());
            Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
            Entry<String, Object> entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
            assertEquals(3.0, entry.getValue());

            QueryResponse response2 = response.get(1);
            Map<String, String> tags2 = response2.getTags();
            assertEquals(2, tags2.size());
            assertTrue(tags2.containsKey("rack"));
            assertTrue(tags2.get("rack").equals("r1"));
            assertTrue(tags.containsKey("tag3"));
            assertTrue(tags.get("tag3").equals("value3"));
            Map<String, Object> dps2 = response2.getDps();
            assertEquals(1, dps2.size());
            Iterator<Entry<String, Object>> entries2 = dps2.entrySet().iterator();
            Entry<String, Object> entry2 = entries2.next();
            assertEquals(Long.toString((TEST_TIME / 1000)), entry2.getKey());
            assertEquals(1.0, entry2.getValue());
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testGetVersion() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            String response = query("https://127.0.0.1:54322/version", "application/json");
            assertNotNull(response);
            assertEquals(VersionRequest.VERSION, response);
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testPutMetric() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            Metric m = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D).tag(new Tag("tag1", "value1"))
                    .build();
            new Metric();
            URL url = new URL("https://127.0.0.1:54322/api/put");
            HttpsURLConnection con = getUrlConnection(url);
            con.setRequestMethod("POST");
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type", "application/json");
            String requestJSON = JsonUtil.getObjectMapper().writeValueAsString(m);
            System.out.println(requestJSON);
            con.setRequestProperty("Content-Length", String.valueOf(requestJSON.length()));
            OutputStream wr = con.getOutputStream();
            wr.write(requestJSON.getBytes(UTF_8));
            int responseCode = con.getResponseCode();
            Assert.assertEquals(200, responseCode);
        } finally {
            s.shutdown();
        }
    }
}

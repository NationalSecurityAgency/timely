package timely;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.model.Metric;
import timely.api.model.Tag;
import timely.api.query.request.QueryRequest;
import timely.api.query.request.QueryRequest.SubQuery;
import timely.api.query.response.QueryResponse;
import timely.test.IntegrationTest;
import timely.util.JsonUtil;

import com.fasterxml.jackson.databind.JavaType;

@Category(IntegrationTest.class)
public class TimelyIntegrationTest {

    private static class NotSuccessfulException extends Exception {

        private static final long serialVersionUID = 1L;
    }

    private static final Logger LOG = LoggerFactory.getLogger(TimelyIntegrationTest.class);
    private static final Long TEST_TIME = System.currentTimeMillis();

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static MiniAccumuloCluster mac = null;
    private static File conf = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        temp.create();
        final MiniAccumuloConfig macConfig = new MiniAccumuloConfig(temp.newFolder("mac"), "secret");
        mac = new MiniAccumuloCluster(macConfig);
        mac.start();
        conf = temp.newFile("config.properties");
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write(Configuration.IP + "=127.0.0.1\n");
            writer.write(Configuration.PUT_PORT + "=54321\n");
            writer.write(Configuration.QUERY_PORT + "=54322\n");
            writer.write(Configuration.ZOOKEEPERS + "=" + mac.getZooKeepers() + "\n");
            writer.write(Configuration.INSTANCE_NAME + "=" + mac.getInstanceName() + "\n");
            writer.write(Configuration.USERNAME + "=root\n");
            writer.write(Configuration.PASSWORD + "=secret\n");
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        mac.stop();
    }

    @Before
    public void setup() throws Exception {
        Connector con = mac.getConnector("root", "secret");
        con.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    con.tableOperations().delete(t);
                } catch (Exception e) {
                }
            }
        });
    }

    @Test
    public void testPut() throws Exception {
        final TestServer m = new TestServer(conf);
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n");
            writer.flush();
            while (1 != m.getPutRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, m.getPutRequests().getResponses().size());
            Assert.assertEquals(Metric.class, m.getPutRequests().getResponses().get(0).getClass());
            final Metric actual = (Metric) m.getPutRequests().getResponses().get(0);
            final Metric expected = new Metric();
            expected.setMetric("sys.cpu.user");
            expected.setTimestamp(TEST_TIME);
            expected.setValue(1.0);
            final List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("tag1", "value1"));
            tags.add(new Tag("tag2", "value2"));
            expected.setTags(tags);
            Assert.assertEquals(expected, actual);
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPutMultiple() throws Exception {

        final TestServer m = new TestServer(conf);
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n" + "put sys.cpu.idle "
                    + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4\n");
            writer.flush();
            while (2 != m.getPutRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, m.getPutRequests().getResponses().size());
            Assert.assertEquals(Metric.class, m.getPutRequests().getResponses().get(0).getClass());
            Metric actual = (Metric) m.getPutRequests().getResponses().get(0);
            Metric expected = new Metric();
            expected.setMetric("sys.cpu.user");
            expected.setTimestamp(TEST_TIME);
            expected.setValue(1.0);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("tag1", "value1"));
            tags.add(new Tag("tag2", "value2"));
            expected.setTags(tags);
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(Metric.class, m.getPutRequests().getResponses().get(1).getClass());
            actual = (Metric) m.getPutRequests().getResponses().get(1);
            expected = new Metric();
            expected.setMetric("sys.cpu.idle");
            expected.setTimestamp(TEST_TIME + 1);
            expected.setValue(1.0);
            tags = new ArrayList<>();
            tags.add(new Tag("tag3", "value3"));
            tags.add(new Tag("tag4", "value4"));
            expected.setTags(tags);
            Assert.assertEquals(expected, actual);

        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPutInvalidTimestamp() throws Exception {
        final TestServer m = new TestServer(conf);
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));) {
            writer.write("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n");
            writer.flush();
            sleepUninterruptibly(1, TimeUnit.SECONDS);
            Assert.assertEquals(0, m.getPutRequests().getCount());
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPersistence() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4");
            sleepUninterruptibly(5, TimeUnit.SECONDS);
        } finally {
            m.shutdown();
        }
        final ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        final Connector connector = inst.getConnector("root", new PasswordToken("secret".getBytes(UTF_8)));
        assertTrue(connector.namespaceOperations().exists("timely"));
        assertTrue(connector.tableOperations().exists("timely.metrics"));
        assertTrue(connector.tableOperations().exists("timely.meta"));
        int count = 0;
        final DoubleLexicoder valueDecoder = new DoubleLexicoder();
        for (final Entry<Key, Value> entry : connector.createScanner("timely.metrics", Authorizations.EMPTY)) {
            LOG.info("Entry: " + entry);
            final double value = valueDecoder.decode(entry.getValue().get());
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
        count = 0;
        for (final Entry<Key, Value> entry : connector.createScanner("timely.meta", Authorizations.EMPTY)) {
            LOG.info("Meta entry: " + entry);
            count++;
        }
        assertEquals(10, count);
        // count w/out versioning iterator to make sure that the optimization
        // for writing is working
        connector.tableOperations().removeIterator("timely.meta", "vers", EnumSet.of(IteratorScope.scan));
        // wait for zookeeper propagation
        sleepUninterruptibly(3, TimeUnit.SECONDS);
        count = 0;
        for (final Entry<Key, Value> entry : connector.createScanner("timely.meta", Authorizations.EMPTY)) {
            LOG.info("Meta no vers iter: " + entry);
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    public void testSuggest() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4", "zzzz 1234567892 1.0 host=localhost");
            sleepUninterruptibly(10, TimeUnit.SECONDS);

            String suggest = "http://localhost:54322/api/suggest?";
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
            m.shutdown();
        }
    }

    @Test
    public void testMetrics() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4", "zzzz 1234567892 1.0 host=localhost");
            sleepUninterruptibly(10, TimeUnit.SECONDS);

            String metrics = "http://localhost:54322/api/metrics";
            // Test prefix matching
            String result = query(metrics);
            assertTrue(result.contains("<td>sys.cpu.user</td>"));
            assertTrue(result.contains("<td>tag1=value1 tag2=value2 </td>"));
            assertTrue(result.contains("<td>sys.cpu.idle</td>"));
            assertTrue(result.contains("<td>tag3=value3 tag4=value4 </td>"));
            assertTrue(result.contains("<td>zzzz</td>"));
            assertTrue(result.contains("<td>host=localhost </td>"));
        } finally {
            m.shutdown();
        }

    }

    @Test
    public void testLookup() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
            sleepUninterruptibly(8, TimeUnit.SECONDS);

            String suggest = "http://localhost:54322/api/search/lookup?";

            // Test a known query
            String result = query(suggest + "m=sys.cpu.idle%7Btag3%3D*%7D");
            assertTrue(result, result.indexOf("\"results\":[{\"tags\":{\"tag3\":\"value3\"}") >= 0);

            // Test a fail
            result = query(suggest + "m=sys.cpu.idle%7Btag3%3Dstupid%7D");
            assertTrue(result.indexOf("\"results\":[]") >= 0);

            // Test multiple results
            result = query(suggest + "m=sys.cpu.idle%7Btag3%3D*,tag4%3D*%7D");
            assertTrue(result, result.indexOf("\"results\":[{\"tags\":{\"tag3\":\"value3\"}") >= 0);
            assertTrue(result, result.indexOf("{\"tags\":{\"tag4\":\"value4\"}") >= 0);

            // Find a tag only in the metric that matches
            result = query(suggest + "m=sys.cpu.idle%7Btag1%3D*%7D");
            assertTrue(result, result.indexOf("\"results\":[]") >= 0);

        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testQueryWithMsResolution() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
            sleepUninterruptibly(8, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            request.setMsResolution(true);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            subQuery.setTags(Collections.singletonMap("tag3", "value3"));
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("http://127.0.0.1:54322/api/query", request);
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
            m.shutdown();
        }
    }

    @Test
    public void testQueryWithoutMsResolution() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
            sleepUninterruptibly(8, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            subQuery.setTags(Collections.singletonMap("tag3", "value3"));
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("http://127.0.0.1:54322/api/query", request);
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
            m.shutdown();
        }
    }

    @Test
    public void testQueryWithNoTags() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 1000) + " 3.0 tag3=value3 tag4=value4");
            sleepUninterruptibly(8, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("http://127.0.0.1:54322/api/query", request);
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
            m.shutdown();
        }
    }

    @Test(expected = NotSuccessfulException.class)
    public void testQueryWithNoMatchingTags() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");
            sleepUninterruptibly(8, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            subQuery.setTags(Collections.singletonMap("rack", "r3"));
            request.addQuery(subQuery);
            query("http://127.0.0.1:54322/api/query", request, 400);
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testQueryWithTagWildcard() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");
            sleepUninterruptibly(8, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            subQuery.setTags(Collections.singletonMap("rack", "r*"));
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("http://127.0.0.1:54322/api/query", request);
            assertEquals(2, response.size());
            QueryResponse response1 = response.get(0);
            Map<String, String> tags = response1.getTags();
            assertEquals(1, tags.size());
            assertTrue(tags.containsKey("rack"));
            assertTrue(tags.get("rack").equals("r2"));
            Map<String, Object> dps = response1.getDps();
            assertEquals(1, dps.size());
            Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
            Entry<String, Object> entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
            assertEquals(3.0, entry.getValue());

            QueryResponse response2 = response.get(1);
            Map<String, String> tags2 = response2.getTags();
            assertEquals(1, tags2.size());
            assertTrue(tags2.containsKey("rack"));
            assertTrue(tags2.get("rack").equals("r1"));
            Map<String, Object> dps2 = response2.getDps();
            assertEquals(1, dps2.size());
            Iterator<Entry<String, Object>> entries2 = dps2.entrySet().iterator();
            Entry<String, Object> entry2 = entries2.next();
            assertEquals(Long.toString((TEST_TIME / 1000)), entry2.getKey());
            assertEquals(1.0, entry2.getValue());
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testQueryWithTagWildcard2() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");
            sleepUninterruptibly(8, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.idle");
            subQuery.setTags(Collections.singletonMap("rack", "*"));
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("http://127.0.0.1:54322/api/query", request);
            assertEquals(2, response.size());
            QueryResponse response1 = response.get(0);
            Map<String, String> tags = response1.getTags();
            assertEquals(1, tags.size());
            assertTrue(tags.containsKey("rack"));
            assertTrue(tags.get("rack").equals("r2"));
            Map<String, Object> dps = response1.getDps();
            assertEquals(1, dps.size());
            Iterator<Entry<String, Object>> entries = dps.entrySet().iterator();
            Entry<String, Object> entry = entries.next();
            assertEquals(Long.toString((TEST_TIME / 1000) + 1), entry.getKey());
            assertEquals(3.0, entry.getValue());

            QueryResponse response2 = response.get(1);
            Map<String, String> tags2 = response2.getTags();
            assertEquals(1, tags2.size());
            assertTrue(tags2.containsKey("rack"));
            assertTrue(tags2.get("rack").equals("r1"));
            Map<String, Object> dps2 = response2.getDps();
            assertEquals(1, dps2.size());
            Iterator<Entry<String, Object>> entries2 = dps2.entrySet().iterator();
            Entry<String, Object> entry2 = entries2.next();
            assertEquals(Long.toString((TEST_TIME / 1000)), entry2.getKey());
            assertEquals(1.0, entry2.getValue());
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testUnhandledRequest() throws Exception {
        final Server m = new Server(conf);
        try {
            URL url = new URL("http://127.0.0.1:54322/favicon.ico");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            int responseCode = con.getResponseCode();
            assertEquals(404, responseCode);
        } finally {
            m.shutdown();
        }
    }

    private void put(String... lines) throws Exception {
        StringBuffer format = new StringBuffer();
        for (String line : lines) {
            format.append("put ");
            format.append(line);
            format.append("\n");
        }
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write(format.toString());
            writer.flush();
        }
    }

    private String query(String getRequest) throws Exception {
        URL url = new URL(getRequest);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        int responseCode = con.getResponseCode();
        assertEquals(200, responseCode);
        String result = IOUtils.toString(con.getInputStream(), UTF_8);
        LOG.info("Result is {}", result);
        return result;
    }

    private List<QueryResponse> query(String location, QueryRequest request) throws Exception {
        return query(location, request, 200);
    }

    private List<QueryResponse> query(String location, QueryRequest request, int expectedResponseCode) throws Exception {
        URL url = new URL(location);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        String requestJSON = JsonUtil.getObjectMapper().writeValueAsString(request);
        con.setRequestProperty("Content-Length", String.valueOf(requestJSON.length()));
        OutputStream wr = con.getOutputStream();
        wr.write(requestJSON.getBytes(UTF_8));
        int responseCode = con.getResponseCode();
        Assert.assertEquals(expectedResponseCode, responseCode);
        if (200 == responseCode) {
            String result = IOUtils.toString(con.getInputStream(), UTF_8);
            LOG.info("Result is {}", result);
            JavaType type = JsonUtil.getObjectMapper().getTypeFactory()
                    .constructCollectionType(List.class, QueryResponse.class);
            return JsonUtil.getObjectMapper().readValue(result, type);
        } else {
            throw new NotSuccessfulException();
        }
    }

}

package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslClientContext;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import timely.Configuration;
import timely.Server;
import timely.api.query.request.QueryRequest;
import timely.api.query.request.QueryRequest.SubQuery;
import timely.api.query.response.QueryResponse;
import timely.auth.AuthCache;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;

@Category(IntegrationTest.class)
public class OneWaySSLAnonAccessIT extends BaseQueryIT {

    private static final Long TEST_TIME = System.currentTimeMillis();

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static MiniAccumuloCluster mac = null;
    private static File conf = null;
    private static File clientTrustStoreFile = null;

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        builder.sslProvider(SslProvider.JDK);
        builder.trustManager(clientTrustStoreFile); // Trust the server cert
        SslContext ctx = builder.build();
        Assert.assertEquals(JdkSslClientContext.class, ctx.getClass());
        JdkSslContext jdk = (JdkSslContext) ctx;
        SSLContext jdkSslContext = jdk.context();
        return jdkSslContext.getSocketFactory();
    }

    protected static void setupSSL(TestConfiguration config) throws Exception {
        SelfSignedCertificate serverCert = new SelfSignedCertificate();
        config.put(Configuration.SSL_CERTIFICATE_FILE, serverCert.certificate().getAbsolutePath());
        clientTrustStoreFile = serverCert.certificate().getAbsoluteFile();
        config.put(Configuration.SSL_PRIVATE_KEY_FILE, serverCert.privateKey().getAbsolutePath());
        config.put(Configuration.SSL_USE_OPENSSL, "false");
        config.put(Configuration.SSL_USE_GENERATED_KEYPAIR, "false");
        config.put(Configuration.ALLOW_ANONYMOUS_ACCESS, "true");
    }

    @Override
    protected HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception {
        // No username/password needed for anonymous access
        return getUrlConnection(url);
    }

    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
        con.setHostnameVerifier(new HostnameVerifier() {

            @Override
            public boolean verify(String arg0, SSLSession arg1) {
                return true;
            }
        });
        return con;
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        temp.create();
        final MiniAccumuloConfig macConfig = new MiniAccumuloConfig(temp.newFolder("mac"), "secret");
        mac = new MiniAccumuloCluster(macConfig);
        mac.start();
        conf = temp.newFile("config.properties");
        TestConfiguration config = TestConfiguration.createMinimalConfigurationForTest();
        config.put(Configuration.INSTANCE_NAME, mac.getInstanceName());
        config.put(Configuration.ZOOKEEPERS, mac.getZooKeepers());
        setupSSL(config);
        config.toConfiguration(conf);
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

    @After
    public void tearDown() throws Exception {
        AuthCache.resetSessionMaxAge();
    }

    @Test
    public void testSuggest() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4", "zzzz 1234567892 1.0 host=localhost");
            sleepUninterruptibly(10, TimeUnit.SECONDS);

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
            m.shutdown();
        }
    }

    @Test
    public void testMetrics() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 viz=(a|b|c)", "zzzz 1234567892 1.0 host=localhost");
            sleepUninterruptibly(10, TimeUnit.SECONDS);

            String metrics = "https://localhost:54322/api/metrics";
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

            String suggest = "https://localhost:54322/api/search/lookup?";

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
            query("https://127.0.0.1:54322/api/query", request, 400);
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
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
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
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
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

    @Test(expected = NotSuccessfulException.class)
    public void testUnhandledRequest() throws Exception {
        final Server m = new Server(conf);
        try {
            query("https://127.0.0.1:54322/favicon.ico", 404);
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testQueryWithVisibility() throws Exception {
        final Server m = new Server(conf);
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1000)
                    + " 3.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 2000)
                    + " 2.0 tag1=value1 tag3=value3 viz=secret");
            sleepUninterruptibly(8, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + 6000);
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.user");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(0, tags.size());
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(2, dps.size());
        } finally {
            m.shutdown();
        }
    }
}

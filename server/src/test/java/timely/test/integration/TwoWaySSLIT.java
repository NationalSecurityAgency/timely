package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import timely.Server;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.timeseries.QueryResponse;
import timely.auth.AuthCache;
import timely.configuration.Configuration;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;

/**
 *
 * Tests that Two way SSL without anonymous access works.
 *
 */
@Category(IntegrationTest.class)
public class TwoWaySSLIT extends TwoWaySSLBase {

    private static final Long TEST_TIME = System.currentTimeMillis();

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static MiniAccumuloCluster mac = null;
    private static Configuration conf = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        final MiniAccumuloConfig macConfig = new MiniAccumuloConfig(temp.newFolder("mac"), "secret");
        mac = new MiniAccumuloCluster(macConfig);
        mac.start();
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getAccumulo().setInstanceName(mac.getInstanceName());
        conf.getAccumulo().setZookeepers(mac.getZooKeepers());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        mac.stop();
    }

    @Before
    public void setup() throws Exception {
        Connector con = mac.getConnector("root", "secret");
        con.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E", "F"));
        con.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    con.tableOperations().delete(t);
                } catch (Exception e) {
                }
            }
        });
        setupSSL(conf, true);
    }

    @After
    public void tearDown() throws Exception {
        AuthCache.resetConfiguration();
    }

    @Test
    public void testBasicAuthLogin() throws Exception {
        final Server s = new Server(conf);
        s.run(getSslContext());
        try {
            String metrics = "https://localhost:54322/api/metrics";
            query(metrics);
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testQueryWithVisibilityWithoutCache() throws Exception {
        conf.getCache().setEnabled(false);
        testQueryWithVisibility(conf);
    }

    @Test
    public void testQueryWithVisibilityWithCache() throws Exception {
        conf.getCache().setEnabled(true);
        testQueryWithVisibility(conf);
    }

    public void testQueryWithVisibility(Configuration conf) throws Exception {
        final Server s = new Server(conf);
        s.run(getSslContext());
        try {
      // @formatter:off
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                "sys.cpu.user " + (TEST_TIME + 1000) + " 2.0 tag1=value1 tag2=value2",
                "sys.cpu.user " + (TEST_TIME + 2000) + " 3.0 tag1=value1 tag3=value3 viz=A",
                "sys.cpu.user " + (TEST_TIME + 3000) + " 4.0 tag1=value1 tag3=value3 viz=D",
                "sys.cpu.user " + (TEST_TIME + 3000) + " 5.0 tag1=value1 tag3=value3 viz=G");
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
                request.setStart(TEST_TIME);
                request.setEnd(TEST_TIME + 6000);
            QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
                subQuery.setMetric("sys.cpu.user");
                subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            // @formatter:on

            String metrics = "https://127.0.0.1:54322/api/query";
            List<QueryResponse> response = query(metrics, request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(0, tags.size());
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(3, dps.size());
        } finally {
            s.shutdown();
        }
    }

}

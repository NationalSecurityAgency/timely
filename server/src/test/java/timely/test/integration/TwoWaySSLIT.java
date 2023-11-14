package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
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

    private static final Long TEST_TIME = (System.currentTimeMillis() / 1000) * 1000;

    @Before
    public void setup() throws Exception {
        accumuloClient.securityOperations().changeUserAuthorizations("root",
                new Authorizations("A", "B", "C", "D", "E", "F"));
        accumuloClient.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    accumuloClient.tableOperations().delete(t);
                } catch (Exception e) {
                }
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        AuthCache.resetConfiguration();
    }

    @Test
    public void testBasicAuthLogin() throws Exception {
        startServer();
        String metrics = "https://localhost:54322/api/metrics";
        query(metrics);
        stopServer();
    }

    @Test
    public void testQueryWithVisibilityWithoutCache() throws Exception {
        conf.getCache().setEnabled(false);
        startServer();
        testQueryWithVisibility(conf);
        stopServer();
    }

    @Test
    public void testQueryWithVisibilityWithCache() throws Exception {
        conf.getCache().setEnabled(true);
        startServer();
        testQueryWithVisibility(conf);
        stopServer();
    }

    public void testQueryWithVisibility(Configuration conf) throws Exception {
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
    }

}

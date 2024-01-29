package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;

/**
 *
 * Tests that OneWay SSL with anonymous access works.
 *
 */
@Category(IntegrationTest.class)
public class OneWaySSLAnonAccessIT extends OneWaySSLBase {

    private static final Long TEST_TIME = (System.currentTimeMillis() / 1000) * 1000;

    @Before
    public void startup() {
        startServer();
    }

    @After
    public void shutdown() {
        stopServer();
    }

    @Test
    public void testQueryWithVisibility() throws Exception {
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1000) + " 3.0 tag1=value1 tag2=value2",
                        "sys.cpu.user " + (TEST_TIME + 2000) + " 2.0 tag1=value1 tag3=value3 viz=secret");
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.user");
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);
        List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
        assertEquals(1, response.size());
        Map<String,String> tags = response.get(0).getTags();
        assertEquals(0, tags.size());
        Map<String,Object> dps = response.get(0).getDps();
        assertEquals(2, dps.size());
    }

}

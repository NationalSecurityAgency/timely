package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import timely.Server;
import timely.api.query.request.QueryRequest;
import timely.api.query.request.QueryRequest.SubQuery;
import timely.api.query.response.QueryResponse;
import timely.test.IntegrationTest;

@Category(IntegrationTest.class)
public class OneWaySSLAnonAccessIT extends OneWaySSLBaseIT {

    private static final Long TEST_TIME = System.currentTimeMillis();

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

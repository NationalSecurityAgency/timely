package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import timely.Server;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.test.IntegrationTest;

@Category(IntegrationTest.class)
public class DataStoreIT extends OneWaySSLBase {

    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long ONE_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long TEST_TIME = System.currentTimeMillis() - ONE_DAY;

    @Test
    public void testDefaultAgeOff() throws Exception {
        HashMap<String, Integer> ageOffSettings = new HashMap<>();
        ageOffSettings.put("default", 1);
        conf.setMetricAgeOffDays(ageOffSettings);

        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + ONE_HOUR)
                    + " 3.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + (ONE_HOUR * 2))
                    + " 2.0 tag1=value1 tag3=value3");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(4, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME);
            request.setEnd(TEST_TIME + (24 * ONE_HOUR));
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.user");
            subQuery.addTag("tag1", ".*");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(1, tags.size());
            assertEquals("value1", tags.get("tag1"));
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(2, dps.size());
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testMultipleAgeOff() throws Exception {
        HashMap<String, Integer> ageOffSettings = new HashMap<>();
        ageOffSettings.put("default", 1);
        ageOffSettings.put("sys.cpu.user", 1);
        conf.setMetricAgeOffDays(ageOffSettings);

        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.idle " + (TEST_TIME - ONE_DAY - (2 * ONE_HOUR)) + " 1.0 tag1=value1 tag2=value2",
                    "sys.cpu.idle " + (TEST_TIME - ONE_DAY - ONE_HOUR) + " 3.0 tag1=value1 tag2=value2",
                    "sys.cpu.idle " + (TEST_TIME - ONE_DAY) + " 2.0 tag1=value1 tag3=value3", "sys.cpu.user "
                            + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + ONE_HOUR)
                            + " 3.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + (ONE_HOUR * 2))
                            + " 2.0 tag1=value1 tag3=value3");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(4, TimeUnit.SECONDS);
            QueryRequest request = new QueryRequest();
            request.setStart(TEST_TIME - ONE_DAY - (2 * ONE_HOUR));
            request.setEnd(TEST_TIME + (24 * ONE_HOUR));
            SubQuery subQuery = new SubQuery();
            subQuery.setMetric("sys.cpu.user");
            subQuery.addTag("tag1", ".*");
            subQuery.setDownsample(Optional.of("1s-max"));
            request.addQuery(subQuery);
            List<QueryResponse> response = query("https://127.0.0.1:54322/api/query", request);
            assertEquals(1, response.size());
            Map<String, String> tags = response.get(0).getTags();
            assertEquals(1, tags.size());
            assertEquals("value1", tags.get("tag1"));
            Map<String, Object> dps = response.get(0).getDps();
            assertEquals(2, dps.size());

            QueryRequest request2 = new QueryRequest();
            request2.setStart(TEST_TIME - ONE_DAY - (2 * ONE_HOUR));
            request2.setEnd(TEST_TIME + (24 * ONE_HOUR));
            SubQuery subQuery2 = new SubQuery();
            subQuery2.setMetric("sys.cpu.idle");
            subQuery2.addTag("tag1", ".*");
            subQuery2.setDownsample(Optional.of("1s-max"));
            request2.addQuery(subQuery2);
            List<QueryResponse> response2 = query("https://127.0.0.1:54322/api/query", request2);
            assertEquals(0, response2.size());

        } finally {
            s.shutdown();
        }
    }

}

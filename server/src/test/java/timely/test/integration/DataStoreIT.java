package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.store.MetricAgeOffIterator;
import timely.store.cache.DataStoreCache;
import timely.test.IntegrationTest;

@Category(IntegrationTest.class)
public class DataStoreIT extends OneWaySSLBase {

    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long ONE_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final Long TEST_TIME = ((System.currentTimeMillis() / 1000) * 1000) - ONE_DAY;

    @After
    public void shutdown() {
        stopServer();
    }

    @Test
    public void testDefaultAgeOffWithoutCache() throws Exception {
        conf.getCache().setEnabled(false);
        conf.setMetricAgeOffDays(Collections.singletonMap(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 1));
        startServer();
        testDefaultAgeOff();
    }

    @Test
    public void testDefaultAgeOffWithCache() throws Exception {
        conf.getCache().setEnabled(true);
        conf.setMetricAgeOffDays(Collections.singletonMap(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 1));
        startServer();
        testDefaultAgeOff();
    }

    @Test
    public void testDefaultAgeOffWithPartialCache() throws Exception {
        conf.getCache().setEnabled(true);
        // This test has three values in relation to TEST_TIME = 24 hours ago
        // TEST_TIME, TEST_TIME + 1 hour, TEST_TIME + 2 hours
        // TEST_TIME should be aged off
        // TEST_TIME + 1 should be retrieved from Accumulo
        // TEST_TIME + 2 should be retrieved from the cache
        HashMap<String,Integer> ageOffHours = new HashMap<>();
        ageOffHours.put(DataStoreCache.DEFAULT_AGEOFF_KEY, 23);
        conf.getCache().setMetricAgeOffHours(ageOffHours);
        conf.setMetricAgeOffDays(Collections.singletonMap(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 1));
        startServer();
        testDefaultAgeOff();
    }

    public void testDefaultAgeOff() throws Exception {
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + ONE_HOUR) + " 3.0 tag1=value1 tag2=value2",
                        "sys.cpu.user " + (TEST_TIME + (ONE_HOUR * 2)) + " 2.0 tag1=value1 tag3=value3");
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
        Map<String,String> tags = response.get(0).getTags();
        assertEquals(1, tags.size());
        assertEquals("value1", tags.get("tag1"));
        Map<String,Object> dps = response.get(0).getDps();
        assertEquals(2, dps.size());
    }

    @Test
    public void testMultipleAgeOffWithoutCache() throws Exception {
        conf.getCache().setEnabled(false);
        conf.setMetricAgeOffDays(Collections.singletonMap(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 1));
        startServer();
        testDefaultAgeOff();
    }

    @Test
    public void testMultipleAgeOffWithCache() throws Exception {
        conf.getCache().setEnabled(true);
        HashMap<String,Integer> ageOffSettings = new HashMap<>();
        ageOffSettings.put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 1);
        ageOffSettings.put("sys.cpu.user", 1);
        conf.setMetricAgeOffDays(ageOffSettings);
        startServer();
        testMultipleAgeOff();
    }

    public void testMultipleAgeOff() throws Exception {

        // @formatter:off
        /*
         * TEST_TIME = ((System.currentTimeMillis() / 1000) * 1000) - ONE_DAY
         * Age off for all metrics is one day = 24 hours
         *
         * lines 1, 2, 3 will age off immediately as they are >= 2 days old
         * line 4 is there because otherwise the meta tags would also age off and we would get a 400 - No Tags Found
         * line 5 is one day old and will age off immediately
         * lines 6 & 7 should be returned as they are not aged off and are within the query range
         *
         */
        // @formatter:on
        put("sys.cpu.idle " + (TEST_TIME - ONE_DAY - (2 * ONE_HOUR)) + " 1.0 tag1=value1 tag2=value2",
                        "sys.cpu.idle " + (TEST_TIME - ONE_DAY - ONE_HOUR) + " 3.0 tag1=value1 tag2=value2",
                        "sys.cpu.idle " + (TEST_TIME - ONE_DAY) + " 2.0 tag1=value1 tag3=value3",
                        "sys.cpu.idle " + (TEST_TIME + (ONE_DAY * 2)) + " 2.0 tag1=value1 tag3=value3",
                        "sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + ONE_HOUR) + " 3.0 tag1=value1 tag2=value2",
                        "sys.cpu.user " + (TEST_TIME + (ONE_HOUR * 2)) + " 2.0 tag1=value1 tag3=value3");
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
        Map<String,String> tags = response.get(0).getTags();
        assertEquals(1, tags.size());
        assertEquals("value1", tags.get("tag1"));
        Map<String,Object> dps = response.get(0).getDps();
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
    }

}

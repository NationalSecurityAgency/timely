package timely.server.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import org.wildfly.common.Assert;

import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.HttpProperties;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"oneWaySsl"})
public class DataStoreTagIteratorIT extends OneWaySSLBase {

    private static final long ONE_DAY = TimeUnit.DAYS.toMillis(1);
    private static final long ONE_HOUR = TimeUnit.HOURS.toMillis(1);
    private static final long TEST_TIME = System.currentTimeMillis() - ONE_DAY;
    private String baseUrl;

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private CacheProperties cacheProperties;

    @Autowired
    private HttpProperties httpProperties;

    @Before
    public void setup() {
        super.setup();
        this.baseUrl = "https://" + httpProperties.getHost() + ":" + httpProperties.getPort();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testTagFilter() throws Exception {
        cacheProperties.setEnabled(false);
        /*
         * TEST_TIME = ((System.currentTimeMillis() / 1000) * 1000) - ONE_DAY Age off for all metrics is one day = 24 hours
         *
         * lines 1, 2, 3 will age off immediately as they are >= 2 days old line 4 is there because otherwise the meta tags would also age off and we would get
         * a 400 - No Tags Found line 5 is one day old and will age off immediately lines 6 & 7 should be returned as they are not aged off and are within the
         * query range
         *
         */
        // @formatter:off
        List<String> hosts = Arrays.asList("h01", "h02", "h03", "h04", "h10", "h11", "h12", "h13", "h14");
        List<String> ns = Arrays.asList("ns01", "ns02", "ns03", "ns04", "ns10", "ns11", "ns12", "ns13", "ns14");
        List<String> instance = Arrays.asList("i01", "i02", "i03", "i10", "i11", "i12");

        for (long t=TEST_TIME; t < TEST_TIME + ONE_HOUR; t += 60000) {
            for (String h : hosts) {
                for (String n : ns) {
                    for (String i : instance) {
                        String tags = "host=" + h + " namespace=" + n + " instance=" + i;
                        put("sys.cpu.idle " + t + " 1.0 " + tags,
                                   "sys.cpu.user " + t + " 1.0 " + tags,
                                   "sys.fan.speed " + t + " 1.0 " + tags);
                    }
                }
            }
        }

        // @formatter:on
        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(4, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + ONE_HOUR);
        SubQuery subQuery = new SubQuery();
        subQuery.setMetric("sys.cpu.user");
        subQuery.addTag("host", "h1.*");
        subQuery.addTag("namespace", "ns0.*");
        subQuery.addTag("instance", ".*");
        subQuery.setDownsample(Optional.of("1m-max"));
        request.addQuery(subQuery);
        List<QueryResponse> response = query(this.baseUrl + "/api/query", request);
        assertEquals(120, response.size());
        for (QueryResponse r : response) {
            Assert.assertTrue(r.getTags().get("host").matches("h1.*"));
            Assert.assertTrue(r.getTags().get("namespace").matches("ns0.*"));
        }
    }
}

package timely.server.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Optional;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.HttpProperties;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;
import timely.util.JsonUtil;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"oneWaySsl"})
public class QueryValueBoundsIT extends OneWaySSLBase {

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private CacheProperties cacheProperties;

    @Autowired
    private HttpProperties httpProperties;

    private String url() {
        return "https://" + httpProperties.getHost() + ":" + httpProperties.getPort() + "/api/query";
    }

    private QueryRequest request(long start) {
        QueryRequest request = new QueryRequest();
        request.setStart(start);
        request.setEnd(start + 5000);
        SubQuery sub = new SubQuery();
        sub.setMetric("value.bounds");
        sub.setAggregator("sum");
        sub.setDownsample(Optional.of("1s-sum"));
        sub.addTag("host", "a");
        sub.setMinValue(0.0);
        sub.setMaxValue(2.0);
        request.addQuery(sub);
        return request;
    }

    private void checkHttpBounds(boolean cacheEnabled, boolean get) throws Exception {
        boolean original = cacheProperties.isEnabled();
        try {
            cacheProperties.setEnabled(cacheEnabled);
            long start = System.currentTimeMillis() - 60000;
            start -= start % 1000;
            put("value.bounds " + start + " -2.0 host=a", "value.bounds " + (start + 1000) + " 0.0 host=a", "value.bounds " + (start + 2000) + " 2.0 host=a",
                            "value.bounds " + (start + 3000) + " 4.0 host=a");
            dataStore.flush();
            dataStoreCache.flushCaches(-1);
            if (cacheEnabled) {
                assertTrue(dataStoreCache.getOldestTimestamp("value.bounds") <= start);
            }
            List<QueryResponse> responses;
            if (get) {
                String json = query(url() + "?start=" + start + "&end=" + (start + 5000) + "&m=sum:1s-sum:value.bounds&minValue=0&maxValue=2");
                responses = JsonUtil.getObjectMapper().readValue(json,
                                JsonUtil.getObjectMapper().getTypeFactory().constructCollectionType(List.class, QueryResponse.class));
            } else {
                responses = query(url(), request(start));
            }
            assertEquals(1, responses.size());
            QueryResponse response = responses.get(0);
            assertEquals("value.bounds", response.getMetric());
            if (!get) {
                assertEquals("a", response.getTags().get("host"));
            }
            assertEquals(2, response.getDps().size());
            assertEquals(0.0, response.getDps().get(Long.toString((start + 1000) / 1000)));
            assertEquals(2.0, response.getDps().get(Long.toString((start + 2000) / 1000)));
        } finally {
            cacheProperties.setEnabled(original);
        }
    }

    @Test
    public void postBoundsWithoutCache() throws Exception {
        checkHttpBounds(false, false);
    }

    @Test
    public void postBoundsWithCache() throws Exception {
        checkHttpBounds(true, false);
    }

    @Test
    public void getBoundsWithoutCache() throws Exception {
        checkHttpBounds(false, true);
    }

    @Test
    public void getBoundsWithCache() throws Exception {
        checkHttpBounds(true, true);
    }
}

package timely.test.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.timeseries.QueryResponse;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.HttpProperties;
import timely.netty.Constants;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import timely.test.TimelyServerTestRule;

/**
 *
 * Tests that Two way SSL without anonymous access works
 *
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"twoWaySsl"})
public class TwoWaySSLIT extends TwoWaySSLBase {

    private static final Long TEST_TIME = (System.currentTimeMillis() / 1000) * 1000;

    @Autowired
    @Rule
    public TimelyServerTestRule testRule;

    @Autowired
    private HttpProperties httpProperties;

    @Autowired
    private CacheProperties cacheProperties;

    @Autowired
    @Qualifier("outboundJDKSslContext")
    private SSLContext outboundSSLContext;

    private String baseUrl;

    @Before
    public void setup() {
        super.setup();
        baseUrl = "https://" + httpProperties.getHost() + ":" + httpProperties.getPort();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    protected SSLSocketFactory getSSLSocketFactory() {
        return outboundSSLContext.getSocketFactory();
    }

    @Override
    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        URL loginURL = new URL(url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/login");
        HttpsURLConnection con = (HttpsURLConnection) loginURL.openConnection();
        con.setHostnameVerifier((host, session) -> true);
        con.setRequestMethod("GET");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        con.connect();
        int responseCode = con.getResponseCode();
        if (401 == responseCode) {
            throw new UnauthorizedUserException();
        }
        Assert.assertEquals(200, responseCode);
        List<String> cookies = con.getHeaderFields().get(HttpHeaderNames.SET_COOKIE.toString());
        Assert.assertEquals(1, cookies.size());
        Cookie sessionCookie = ClientCookieDecoder.STRICT.decode(cookies.get(0));
        Assert.assertEquals(Constants.COOKIE_NAME, sessionCookie.name());
        con = (HttpsURLConnection) url.openConnection();
        con.setRequestProperty(HttpHeaderNames.COOKIE.toString(), sessionCookie.name() + "=" + sessionCookie.value());
        con.setHostnameVerifier((host, session) -> true);
        return con;
    }

    @Test
    public void testQueryWithVisibilityWithoutCache() throws Exception {
        boolean isCacheEnabled = cacheProperties.isEnabled();
        cacheProperties.setEnabled(false);
        testQueryWithVisibility();
        cacheProperties.setEnabled(isCacheEnabled);
    }

    @Test
    public void testQueryWithVisibilityWithCache() throws Exception {
        boolean isCacheEnabled = cacheProperties.isEnabled();
        cacheProperties.setEnabled(true);
        testQueryWithVisibility();
        cacheProperties.setEnabled(isCacheEnabled);
    }

    public void testQueryWithVisibility() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
            "sys.cpu.user " + (TEST_TIME + 1000) + " 3.0 tag1=value1 tag2=value2",
            "sys.cpu.user " + (TEST_TIME + 2000) + " 2.0 tag1=value1 tag3=value3 viz=A",
            "sys.cpu.user " + (TEST_TIME + 3000) + " 2.0 tag1=value1 tag3=value3 viz=D",
            "sys.cpu.user " + (TEST_TIME + 3000) + " 2.0 tag1=value1 tag3=value3 viz=G");
        // @formatter:on
        // Latency in TestConfiguration is 2s, wait for it
        dataStore.flush();
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        QueryRequest request = new QueryRequest();
        request.setStart(TEST_TIME);
        request.setEnd(TEST_TIME + 6000);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setMetric("sys.cpu.user");
        subQuery.setDownsample(Optional.of("1s-max"));
        request.addQuery(subQuery);

        String metrics = baseUrl + "/api/query";
        List<QueryResponse> response = query(metrics, request);
        assertEquals(1, response.size());
        Map<String,String> tags = response.get(0).getTags();
        assertEquals(0, tags.size());
        Map<String,Object> dps = response.get(0).getDps();
        assertEquals(3, dps.size());
    }
}

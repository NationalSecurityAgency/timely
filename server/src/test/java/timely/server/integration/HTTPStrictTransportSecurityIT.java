package timely.server.integration;

import static org.junit.Assert.assertEquals;

import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

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

import io.netty.handler.codec.http.HttpHeaderNames;
import timely.common.configuration.HttpProperties;
import timely.server.netty.http.StrictTransportHandler;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"oneWaySsl"})
public class HTTPStrictTransportSecurityIT extends OneWaySSLBase {

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private HttpProperties httpProperties;

    private String baseHttpUrl;
    private String baseHttpsUrl;

    @Before
    public void setup() {
        super.setup();
        baseHttpUrl = "http://" + httpProperties.getHost() + ":" + httpProperties.getPort();
        baseHttpsUrl = "https://" + httpProperties.getHost() + ":" + httpProperties.getPort();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testHttpRequestGet() throws Exception {
        HttpURLConnection.setFollowRedirects(false);
        URL url = new URL(baseHttpUrl + "/api/metrics");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        int responseCode = con.getResponseCode();
        assertEquals(301, responseCode);
        assertEquals(baseHttpsUrl + "/secure-me", con.getHeaderField(HttpHeaderNames.LOCATION.toString()));
        con.disconnect();
    }

    @Test
    public void testHSTSRequestGet() throws Exception {
        String secureMe = baseHttpsUrl + "/secure-me";
        URL url = new URL(secureMe);
        HttpsURLConnection con = getUrlConnection(url);
        int responseCode = con.getResponseCode();
        assertEquals(404, responseCode);
        assertEquals("max-age=" + httpProperties.getStrictTransportMaxAge(), con.getHeaderField(StrictTransportHandler.HSTS_HEADER_NAME));
    }
}

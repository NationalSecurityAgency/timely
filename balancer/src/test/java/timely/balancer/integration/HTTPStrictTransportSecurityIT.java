package timely.balancer.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import timely.balancer.configuration.BalancerHttpProperties;
import timely.netty.http.HttpStrictTransportSecurityHandler;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class HTTPStrictTransportSecurityIT {

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private BalancerHttpProperties httpProperties;

    @Autowired
    protected SSLContext clientSslContext;

    private String baseHttpUrl;
    private String baseHttpsUrl;
    private String HSTS_HEADER_VALUE;

    @Before
    public void setup() {
        baseHttpUrl = "http://" + httpProperties.getHost() + ":" + httpProperties.getPort();
        baseHttpsUrl = "https://" + httpProperties.getHost() + ":" + httpProperties.getPort();
        HSTS_HEADER_VALUE = String.format("max-age=%d; includeSubDomains; preload", httpProperties.getStrictTransportMaxAge());
    }

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        return clientSslContext.getSocketFactory();
    }

    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
        con.setHostnameVerifier((host, session) -> true);
        return con;
    }

    @Test
    public void testHttpRequest() throws Exception {
        URL url = new URL(baseHttpUrl + "/api/metrics");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        int responseCode = con.getResponseCode();
        assertEquals(403, responseCode);
        // should not have HSTS header on HTTP response
        assertNull(con.getHeaderField(HttpStrictTransportSecurityHandler.HSTS_HEADER_NAME));
        con.disconnect();
    }

    @Test
    public void testHttpsRequestHSTS() throws Exception {
        URL url = new URL(baseHttpsUrl + "/version");
        HttpsURLConnection con = getUrlConnection(url);
        int responseCode = con.getResponseCode();
        assertEquals(200, responseCode);
        assertEquals(HSTS_HEADER_VALUE, con.getHeaderField(HttpStrictTransportSecurityHandler.HSTS_HEADER_NAME));
        con.disconnect();
    }

    @Test
    public void testHttpsRequestNotFound() throws Exception {
        URL url = new URL(baseHttpsUrl);
        HttpsURLConnection con = getUrlConnection(url);
        int responseCode = con.getResponseCode();
        assertEquals(404, responseCode);
        assertEquals(HSTS_HEADER_VALUE, con.getHeaderField(HttpStrictTransportSecurityHandler.HSTS_HEADER_NAME));
        con.disconnect();
    }
}

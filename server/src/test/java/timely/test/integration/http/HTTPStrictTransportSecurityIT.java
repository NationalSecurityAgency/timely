package timely.test.integration.http;

import static org.junit.Assert.assertEquals;

import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

import io.netty.handler.codec.http.HttpHeaderNames;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import timely.netty.http.StrictTransportHandler;
import timely.test.IntegrationTest;
import timely.test.integration.OneWaySSLBase;

@Category(IntegrationTest.class)
public class HTTPStrictTransportSecurityIT extends OneWaySSLBase {

    @Before
    public void startup() {
        startServer();
    }

    @After
    public void shutdown() {
        stopServer();
    }

    @Test
    public void testHttpRequestGet() throws Exception {
        HttpURLConnection.setFollowRedirects(false);
        URL url = new URL("http://127.0.0.1:54322/api/metrics");
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        int responseCode = con.getResponseCode();
        assertEquals(301, responseCode);
        assertEquals("https://localhost:54322/secure-me", con.getHeaderField(HttpHeaderNames.LOCATION.toString()));
        con.disconnect();
    }

    @Test
    public void testHSTSRequestGet() throws Exception {
        String secureMe = "https://127.0.0.1:54322/secure-me";
        URL url = new URL(secureMe);
        HttpsURLConnection con = getUrlConnection(url);
        int responseCode = con.getResponseCode();
        assertEquals(404, responseCode);
        assertEquals("max-age=604800", con.getHeaderField(StrictTransportHandler.HSTS_HEADER_NAME));
    }

}

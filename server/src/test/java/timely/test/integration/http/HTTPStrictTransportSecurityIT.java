package timely.test.integration.http;

import static org.junit.Assert.assertEquals;
import io.netty.handler.codec.http.HttpHeaders.Names;

import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import timely.Server;
import timely.netty.http.StrictTransportHandler;
import timely.test.IntegrationTest;
import timely.test.integration.OneWaySSLBase;

@Category(IntegrationTest.class)
public class HTTPStrictTransportSecurityIT extends OneWaySSLBase {

    @Test
    public void testHttpRequestGet() throws Exception {
        final Server s = new Server(conf);
        try {
            HttpURLConnection.setFollowRedirects(false);
            URL url = new URL("http://127.0.0.1:54322/api/metrics");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            int responseCode = con.getResponseCode();
            assertEquals(301, responseCode);
            assertEquals("https://localhost:54322/secure-me", con.getHeaderField(Names.LOCATION));
            con.disconnect();
        } finally {
            s.shutdown();
        }
    }

    @Test
    public void testHSTSRequestGet() throws Exception {
        final Server s = new Server(conf);
        try {
            String secureMe = "https://127.0.0.1:54322/secure-me";
            URL url = new URL(secureMe);
            HttpsURLConnection con = getUrlConnection(url);
            int responseCode = con.getResponseCode();
            assertEquals(404, responseCode);
            assertEquals("max-age=604800", con.getHeaderField(StrictTransportHandler.HSTS_HEADER_NAME));
        } finally {
            s.shutdown();
        }
    }

}

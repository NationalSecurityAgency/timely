package timely.server.integration;

import java.net.URL;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import timely.netty.Constants;

/**
 * Base test class for SSL with anonymous access
 */
public class TwoWaySSLBase extends QueryBase {

    @Autowired
    @Qualifier("outboundNettySslContext")
    private SslContext outboundSslContext;

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        Assert.assertTrue(outboundSslContext.isClient());
        Assert.assertTrue(outboundSslContext instanceof JdkSslContext);
        JdkSslContext jdk = (JdkSslContext) outboundSslContext;
        SSLContext jdkSslContext = jdk.context();
        return jdkSslContext.getSocketFactory();
    }

    @Override
    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        // Username and password not used in 2way SSL case
        return getUrlConnection(null, null, url);
    }

    @Override
    protected HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        URL loginURL = new URL(url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/login");
        HttpsURLConnection con = (HttpsURLConnection) loginURL.openConnection();
        con.setHostnameVerifier((arg0, arg1) -> true);
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
        con.setHostnameVerifier((arg0, arg1) -> true);
        return con;
    }
}

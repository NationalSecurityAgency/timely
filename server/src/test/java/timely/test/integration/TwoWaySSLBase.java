package timely.test.integration;

import java.net.URL;
import java.util.List;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.junit.After;
import org.junit.Assert;
import timely.auth.AuthCache;
import timely.netty.Constants;

/**
 * Base test class for SSL with anonymous access
 */
public class TwoWaySSLBase extends QueryBase {

    @After
    public void tearDown() throws Exception {
        AuthCache.resetConfiguration();
    }

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        // Use server cert / key on client side
        builder.keyManager(MacITBase.serverCert.key(), (String) null, MacITBase.serverCert.cert());
        builder.sslProvider(SslProvider.JDK);
        builder.trustManager(MacITBase.clientTrustStoreFile); // Trust the server cert
        SslContext ctx = builder.build();
        Assert.assertTrue(ctx.isClient());
        Assert.assertTrue(ctx instanceof JdkSslContext);
        JdkSslContext jdk = (JdkSslContext) ctx;
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
        con.setHostnameVerifier(new HostnameVerifier() {

            @Override
            public boolean verify(String arg0, SSLSession arg1) {
                return true;
            }
        });
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
        con.setHostnameVerifier(new HostnameVerifier() {

            @Override
            public boolean verify(String arg0, SSLSession arg1) {
                return true;
            }
        });
        return con;
    }
}

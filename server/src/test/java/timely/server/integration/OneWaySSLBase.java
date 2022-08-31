package timely.server.integration;

import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.springframework.beans.factory.annotation.Autowired;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import timely.common.configuration.SslHelper;
import timely.common.configuration.SslServerProperties;

/**
 * Base test class for SSL with anonymous access
 */
public class OneWaySSLBase extends QueryBase {

    @Autowired
    private SslServerProperties sslServerProperties;

    protected SSLContext clientSslContext;

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        return clientSslContext.getSocketFactory();
    }

    @Before
    public void setup() {
        super.setup();
        try {
            SslContextBuilder builder = SslContextBuilder.forClient();
            builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
            builder.sslProvider(SslProvider.JDK);
            builder.trustManager(SslHelper.getTrustManagerFactory(sslServerProperties)); // Trust the server cert
            SslContext sslContext = builder.build();
            Assert.assertTrue(sslContext.isClient());
            Assert.assertTrue(sslContext instanceof JdkSslContext);
            JdkSslContext jdk = (JdkSslContext) sslContext;
            clientSslContext = jdk.context();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Override
    protected HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception {
        // No username/password needed for anonymous access
        return getUrlConnection(url);
    }

    @Override
    protected HttpsURLConnection getUrlConnection(URL url) throws Exception {
        HttpsURLConnection.setDefaultSSLSocketFactory(getSSLSocketFactory());
        HttpsURLConnection con = (HttpsURLConnection) url.openConnection();
        con.setHostnameVerifier((host, session) -> true);
        return con;
    }
}

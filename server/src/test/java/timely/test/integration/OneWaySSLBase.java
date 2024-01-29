package timely.test.integration;

import java.io.File;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import timely.auth.AuthCache;
import timely.configuration.Configuration;

/**
 * Base test class for SSL with anonymous access
 */
public class OneWaySSLBase extends QueryBase {

    protected static File clientTrustStoreFile = null;

    @Before
    public void setupOneWaySSLBase() throws Exception {
        setupSSL(conf);
    }

    @After
    public void shutdownOneWaySSLBase() {
        AuthCache.resetConfiguration();
    }

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        builder.sslProvider(SslProvider.JDK);
        builder.trustManager(clientTrustStoreFile); // Trust the server cert
        SslContext ctx = builder.build();
        Assert.assertTrue(ctx.isClient());
        Assert.assertTrue(ctx instanceof JdkSslContext);
        JdkSslContext jdk = (JdkSslContext) ctx;
        SSLContext jdkSslContext = jdk.context();
        return jdkSslContext.getSocketFactory();
    }

    protected static void setupSSL(Configuration config) throws Exception {
        SelfSignedCertificate serverCert = new SelfSignedCertificate();
        clientTrustStoreFile = serverCert.certificate().getAbsoluteFile();
        config.getSecurity().getServerSsl().setKeyStoreFile(serverCert.privateKey().getAbsolutePath());
        config.getSecurity().getServerSsl().setUseOpenssl(false);
        config.getSecurity().getServerSsl().setUseGeneratedKeypair(false);
        config.getSecurity().setAllowAnonymousHttpAccess(true);
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

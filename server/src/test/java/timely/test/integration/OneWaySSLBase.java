package timely.test.integration;

import java.io.File;
import java.io.OutputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.io.FileUtils;
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

    protected static File clientTrustStoreFile;
    private static SelfSignedCertificate serverCert;

    static {
        try {
            serverCert = new SelfSignedCertificate();
            clientTrustStoreFile = serverCert.certificate().getAbsoluteFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void setupOneWaySSLBase() throws Exception {
        setupSSL(conf);
    }

    @After
    public void shutdownOneWaySSLBase() {
        AuthCache.resetConfiguration();
    }

    public SelfSignedCertificate getServerCert() {
        return serverCert;
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

    protected void setupSSL(Configuration config) throws Exception {
        SelfSignedCertificate serverCert = getServerCert();
        String password = "password";

        // write the keyStore with server certificate into a P12 trustStore
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setCertificateEntry("cert", serverCert.cert());
        keyStore.setKeyEntry("key", serverCert.key(), password.toCharArray(), new Certificate[] {serverCert.cert()});
        // write the keyStore with private key and server certificate into a P12
        // keyStore
        File keyStoreP12File = File.createTempFile("keyStore_", ".p12");
        try (OutputStream out = FileUtils.newOutputStream(keyStoreP12File, false)) {
            keyStore.store(out, password.toCharArray());
        }

        config.getSecurity().getServerSsl().setKeyStoreType("PKCS12");
        config.getSecurity().getServerSsl().setKeyStoreFile(keyStoreP12File.getAbsolutePath());
        config.getSecurity().getServerSsl().setKeyStorePassword(password);
        config.getSecurity().getServerSsl().setUseOpenssl(false);
        config.getSecurity().getServerSsl().setUseGeneratedKeypair(false);
        config.getSecurity().getServerSsl().setIgnoreSslHandshakeErrors(true);
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

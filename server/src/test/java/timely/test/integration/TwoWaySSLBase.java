package timely.test.integration;

import java.io.File;
import java.io.OutputStream;
import java.net.URL;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import timely.configuration.Configuration;
import timely.netty.Constants;

/**
 * Base test class for SSL with anonymous access
 */
public class TwoWaySSLBase extends QueryBase {

    private static File clientTrustStoreFile;
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
    public void startupTwoWaySSLBase() throws Exception {
        setupSSL(conf);
    }

    public SelfSignedCertificate getServerCert() {
        return serverCert;
    }

    protected File getClientTrustStoreFile() {
        return clientTrustStoreFile;
    }

    protected SSLSocketFactory getSSLSocketFactory() throws Exception {
        SelfSignedCertificate serverCert = getServerCert();
        File clientTrustStoreFile = getClientTrustStoreFile();
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        // Use server cert / key on client side.
        builder.keyManager(serverCert.key(), (String) null, serverCert.cert());
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
        File trustStoreP12File = File.createTempFile("trustStore_", ".p12");
        try (OutputStream out = FileUtils.newOutputStream(trustStoreP12File, false)) {
            keyStore.store(out, password.toCharArray());
        }

        // write the keyStore with private key and server certificate into a P12
        // keyStore
        keyStore.setKeyEntry("key", serverCert.key(), password.toCharArray(), new Certificate[] { serverCert.cert() });
        File keyStoreP12File = File.createTempFile("keyStore_", ".p12");
        try (OutputStream out = FileUtils.newOutputStream(keyStoreP12File, false)) {
            keyStore.store(out, password.toCharArray());
        }

        config.getSecurity().getServerSsl().setKeyStoreType("PKCS12");
        config.getSecurity().getServerSsl().setKeyStoreFile(keyStoreP12File.getAbsolutePath());
        config.getSecurity().getServerSsl().setKeyStorePassword(password);
        // Needed for 2way SSL
        config.getSecurity().getServerSsl().setTrustStoreType("PKCS12");
        config.getSecurity().getServerSsl().setTrustStoreFile(trustStoreP12File.getAbsolutePath());
        config.getSecurity().getServerSsl().setTrustStorePassword(password);
        config.getSecurity().getServerSsl().setUseOpenssl(false);
        config.getSecurity().getServerSsl().setUseGeneratedKeypair(false);
        config.getSecurity().getServerSsl().setIgnoreSslHandshakeErrors(true);
        config.getSecurity().setAllowAnonymousHttpAccess(false);
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

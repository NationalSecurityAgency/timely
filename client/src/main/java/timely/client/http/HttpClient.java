package timely.client.http;

import javax.net.ssl.SSLContext;

import org.apache.http.client.CookieStore;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.glassfish.tyrus.client.SslContextConfigurator;

public class HttpClient {

    public static SSLContext getSSLContext(String trustStoreFile, String trustStoreType, String trustStorePass,
            String keyStoreFile, String keyStoreType, String keyStorePass) {
        SslContextConfigurator scc = new SslContextConfigurator();
        scc.setTrustStoreFile(trustStoreFile);
        scc.setTrustStoreType(trustStoreType);
        if (null != trustStorePass) {
            scc.setTrustStorePassword(trustStorePass);
        }
        scc.setKeyStoreFile(keyStoreFile);
        scc.setKeyStoreType(keyStoreType);
        if (null != keyStorePass) {
            scc.setKeyStorePassword(keyStorePass);
        }
        return scc.createSSLContext();
    }

    public static CloseableHttpClient get(String trustStoreFile, String trustStoreType, String trustStorePass,
            String keyStoreFile, String keyStoreType, String keyStorePass, CookieStore cookieStore,
            boolean hostVerificationEnabled) {

        SSLContext ssl = getSSLContext(trustStoreFile, trustStoreType, trustStorePass, keyStoreFile, keyStoreType,
                keyStorePass);
        return get(ssl, cookieStore, hostVerificationEnabled);
    }

    public static CloseableHttpClient get(SSLContext ssl, CookieStore cookieStore, boolean hostVerificationEnabled) {
        RequestConfig defaultRequestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build();

        HttpClientBuilder builder = HttpClients.custom().setSSLContext(ssl).setDefaultCookieStore(cookieStore)
                .setDefaultRequestConfig(defaultRequestConfig);
        if (hostVerificationEnabled) {
            builder.setSSLHostnameVerifier(new DefaultHostnameVerifier());
        } else {
            builder.setSSLHostnameVerifier(new NoopHostnameVerifier());
        }
        return builder.build();
    }

}

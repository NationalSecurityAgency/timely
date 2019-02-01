package timely.client.http;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.client.CookieStore;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
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
        return get(ssl, cookieStore, hostVerificationEnabled, false);
    }

    public static CloseableHttpClient get(SSLContext ssl, CookieStore cookieStore, boolean hostVerificationEnabled,
            boolean clientAuth) {
        RequestConfig defaultRequestConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build();
        HostnameVerifier hostnameVerifier;
        if (hostVerificationEnabled) {
            hostnameVerifier = SSLConnectionSocketFactory.getDefaultHostnameVerifier();
        } else {
            hostnameVerifier = new NoopHostnameVerifier();
        }
        HttpClientBuilder builder = HttpClients.custom().setSSLContext(ssl).setDefaultCookieStore(cookieStore)
                .setDefaultRequestConfig(defaultRequestConfig).setSSLHostnameVerifier(hostnameVerifier);

        if (clientAuth) {
            SSLConnectionSocketFactory sslCSF = new SSLConnectionSocketFactory(ssl,
                    new String[] { "TLSv1.1", "TLSv1.2" }, null, hostnameVerifier);
            builder.setSSLSocketFactory(sslCSF);
        }
        return builder.build();
    }

}

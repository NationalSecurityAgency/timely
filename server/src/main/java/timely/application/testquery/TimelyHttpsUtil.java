package timely.application.testquery;

import org.apache.http.HttpHost;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;

import timely.client.http.HttpClient;
import timely.common.configuration.SslClientProperties;

public class TimelyHttpsUtil {

    private CloseableHttpClient client;
    CookieStore cookieStore = new BasicCookieStore();

    public TimelyHttpsUtil(SslClientProperties ssl) {
        String trustStoreFile = ssl.getTrustStoreFile();
        String trustStoreType = ssl.getTrustStoreType();
        String trustStorePass = ssl.getTrustStorePassword();
        String keyStoreFile = ssl.getKeyStoreFile();
        String keyStoreType = ssl.getKeyStoreType();
        String keyStorePass = ssl.getKeyStorePassword();
        client = HttpClient.get(trustStoreFile, trustStoreType, trustStorePass, keyStoreFile, keyStoreType, keyStorePass, cookieStore, false);
    }

    public CloseableHttpResponse query(MetricQuery query, String host, int port) {

        CloseableHttpResponse response = null;

        try {
            HttpHost httpHost = new HttpHost(host, port, "https");
            HttpPost httpPost = new HttpPost("https://" + host + ":" + Integer.toString(port) + "/api/query");
            httpPost.setEntity(new StringEntity(query.getJson()));
            response = client.execute(httpHost, httpPost);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

}

package timely.testing;

import org.apache.http.HttpHost;
import org.apache.http.client.CookieStore;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import timely.client.http.HttpClient;

public class TimelyHttpsUtil {

    private CloseableHttpClient client = null;
    CookieStore cookieStore = new BasicCookieStore();

    public TimelyHttpsUtil(String trustStoreFile, String trustStoreType, String trustStorePass, String keyStoreFile,
            String keyStoreType, String keyStorePass) {
        client = HttpClient.get(trustStoreFile, trustStoreType, trustStorePass, keyStoreFile, keyStoreType,
                keyStorePass, cookieStore, false);
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

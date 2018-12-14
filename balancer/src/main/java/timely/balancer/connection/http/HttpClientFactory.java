package timely.balancer.connection.http;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import timely.balancer.connection.TimelyBalancedHost;

public class HttpClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost, CloseableHttpClient> {

    private final SSLContext sslContext;

    public HttpClientFactory(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public PooledObject<CloseableHttpClient> makeObject(TimelyBalancedHost k) throws Exception {
        BasicCookieStore cookieJar = new BasicCookieStore();
        return new DefaultPooledObject<>(timely.client.http.HttpClient.get(this.sslContext, cookieJar, false));
    }

    @Override
    public void destroyObject(TimelyBalancedHost k, PooledObject<CloseableHttpClient> o) throws Exception {

    }

    @Override
    public boolean validateObject(TimelyBalancedHost k, PooledObject<CloseableHttpClient> o) {
        return true;
    }

    @Override
    public void activateObject(TimelyBalancedHost k, PooledObject<CloseableHttpClient> o) throws Exception {

    }

    @Override
    public void passivateObject(TimelyBalancedHost k, PooledObject<CloseableHttpClient> o) throws Exception {

    }
}

package timely.balancer.connection.http;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCookieStore;
import timely.balancer.connection.TimelyBalancedHost;

import javax.net.ssl.SSLContext;

public class HttpClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost, HttpClient> {

    private final SSLContext sslContext;

    public HttpClientFactory(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Override
    public PooledObject<HttpClient> makeObject(TimelyBalancedHost k) throws Exception {
        BasicCookieStore cookieJar = new BasicCookieStore();
        return new DefaultPooledObject<>(timely.client.http.HttpClient.get(this.sslContext, cookieJar, false));
    }

    @Override
    public void destroyObject(TimelyBalancedHost k, PooledObject<HttpClient> o) throws Exception {

    }

    @Override
    public boolean validateObject(TimelyBalancedHost k, PooledObject<HttpClient> o) {
        return true;
    }

    @Override
    public void activateObject(TimelyBalancedHost k, PooledObject<HttpClient> o) throws Exception {

        // try {
        // o.getObject().open();
        // } catch (IOException e) {
        // throw new IOException("Unable to connect to " + k.getHost() + ":" +
        // k.getHttpPort());
        // }
    }

    @Override
    public void passivateObject(TimelyBalancedHost k, PooledObject<HttpClient> o) throws Exception {
        // try {
        // o.getObject().flush();
        // } catch (Exception e) {
        // throw new IOException("Error flushing connection to " + k.getHost() +
        // ":" + k.getHttpPort());
        // }
        // try {
        // o.getObject().flush();
        // o.getObject().close();
        // } catch (IOException e) {
        // throw new IOException("Error closing connection to " + k.getHost() +
        // ":" + k.getHttpPort());
        // }
    }
}

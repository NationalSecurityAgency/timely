package timely.balancer.connection.http;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;

//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;

public class HttpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost, CloseableHttpClient> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpClientPool.class);

    public HttpClientPool(BalancerConfiguration config, SSLContext sslContext) {
        super(new HttpClientFactory(sslContext), config.getHttpClientPool());
    }

    // private Map<TimelyBalancedHost, Integer> checkedOut = new
    // ConcurrentHashMap<>();

    // @Override
    // public CloseableHttpClient borrowObject(TimelyBalancedHost key) throws
    // Exception {
    // CloseableHttpClient client = super.borrowObject(key);
    // Integer n;
    // synchronized (checkedOut) {
    // n = checkedOut.get(key);
    // if (n == null) {
    // n = 0;
    // }
    // n++;
    // checkedOut.put(key, n);
    // }
    // LOG.trace("Borrowing HttpClient for key " + key.getHost() + ":" +
    // key.getHttpPort() + " " + getNumActive(key)
    // + ":" + getNumIdle(key) + ":" + n);
    // return client;
    // }
    //
    // @Override
    // public void returnObject(TimelyBalancedHost key, CloseableHttpClient obj)
    // {
    // super.returnObject(key, obj);
    // Integer n;
    // synchronized (checkedOut) {
    // n = checkedOut.get(key);
    // n--;
    // checkedOut.put(key, n);
    // }
    // LOG.trace("Returning HttpClient for key " + key.getHost() + ":" +
    // key.getHttpPort() + " " + getNumActive(key)
    // + ":" + getNumIdle(key) + ":" + n);
    // }
}

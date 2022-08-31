package timely.balancer.connection.http;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.impl.client.CloseableHttpClient;

import timely.balancer.connection.TimelyBalancedHost;
import timely.client.http.HttpClient;
import timely.common.configuration.SslClientProperties;

public class HttpClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost,CloseableHttpClient> {

    private final SSLContext sslContext;
    private SslClientProperties sslClientProperties;

    public HttpClientFactory(SSLContext sslContext, SslClientProperties sslClientProperties) {
        this.sslContext = sslContext;
        this.sslClientProperties = sslClientProperties;
    }

    @Override
    public PooledObject<CloseableHttpClient> makeObject(TimelyBalancedHost k) throws Exception {
        // disable cookie management because we are sharing connections
        return new DefaultPooledObject<>(
                        HttpClient.get(this.sslContext, null, sslClientProperties.isHostVerificationEnabled(), sslClientProperties.isUseClientCert()));
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

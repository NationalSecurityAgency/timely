package timely.balancer.connection.http;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.impl.client.CloseableHttpClient;

import timely.balancer.connection.TimelyBalancedHost;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.SslClientProperties;

public class HttpClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost,CloseableHttpClient> {

    private final SSLContext sslContext;
    private final SecurityProperties security;
    private SslClientProperties sslClientProperties;

    public HttpClientFactory(SecurityProperties security, SslClientProperties sslClientProperties, SSLContext sslContext) {
        this.security = security;
        this.sslContext = sslContext;
        this.sslClientProperties = sslClientProperties;
    }

    @Override
    public PooledObject<CloseableHttpClient> makeObject(TimelyBalancedHost k) throws Exception {
        // disable cookie management because we are sharing connections
        return new DefaultPooledObject<>(timely.client.http.HttpClient.get(this.sslContext, null, sslClientProperties.isHostVerificationEnabled(),
                        sslClientProperties.isUseClientCert()));
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

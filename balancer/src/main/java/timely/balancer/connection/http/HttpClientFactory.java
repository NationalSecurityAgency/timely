package timely.balancer.connection.http;

import javax.net.ssl.SSLContext;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.impl.client.CloseableHttpClient;

import timely.balancer.configuration.BalancerSecurity;
import timely.balancer.connection.TimelyBalancedHost;

public class HttpClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost,CloseableHttpClient> {

    private final SSLContext sslContext;
    private final BalancerSecurity security;

    public HttpClientFactory(BalancerSecurity security, SSLContext sslContext) {
        this.security = security;
        this.sslContext = sslContext;
    }

    @Override
    public PooledObject<CloseableHttpClient> makeObject(TimelyBalancedHost k) throws Exception {
        // disable cookie management because we are sharing connections
        return new DefaultPooledObject<>(timely.client.http.HttpClient.get(this.sslContext, null, security.getClientSsl().isHostVerificationEnabled(),
                        security.getClientSsl().isUseClientCert()));
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

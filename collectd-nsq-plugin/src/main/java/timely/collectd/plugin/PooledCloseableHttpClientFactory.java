package timely.collectd.plugin;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.util.concurrent.TimeUnit;

public class PooledCloseableHttpClientFactory implements PooledObjectFactory {

    private int connectionRequestTimeout = 10000; // 10 seconds in milliseconds
    private int connectTimeout = 30000; // 30 seconds in milliseconds
    private int socketTimeout = 60000; // 60 seconds in milliseconds
    private long connectionTimeToLive = 300000; // 300 seconds in milliseconds

    @Override
    public PooledObject makeObject() throws Exception {
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(connectionRequestTimeout)
                .setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout).build();
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setDefaultRequestConfig(requestConfig);
        builder.setConnectionTimeToLive(connectionTimeToLive, TimeUnit.MILLISECONDS);
        return new DefaultPooledObject(builder.build());
    }

    @Override
    public void destroyObject(PooledObject pooledObject) throws Exception {
        try {
            ((CloseableHttpClient) pooledObject.getObject()).close();
        } catch (Exception e) {
            // do nothing
        } finally {
            pooledObject.invalidate();
            ;
        }
    }

    @Override
    public boolean validateObject(PooledObject pooledObject) {
        return pooledObject.getObject() != null;
    }

    @Override
    public void activateObject(PooledObject pooledObject) throws Exception {
        // do nothing
    }

    @Override
    public void passivateObject(PooledObject pooledObject) throws Exception {
        // do nothing
    }

    public void setConnectionRequestTimeout(int connectionRequestTimeout) {
        this.connectionRequestTimeout = connectionRequestTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public void setConnectionTimeToLive(long connectionTimeToLive) {
        this.connectionTimeToLive = connectionTimeToLive;
    }
}

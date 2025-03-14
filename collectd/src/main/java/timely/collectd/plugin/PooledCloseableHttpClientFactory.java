package timely.collectd.plugin;

import java.util.concurrent.TimeUnit;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.collectd.api.OConfigItem;

public class PooledCloseableHttpClientFactory implements PooledObjectFactory<CloseableHttpClient> {

    private int connectionRequestTimeout = 10000; // 10 seconds in milliseconds
    private int connectTimeout = 30000; // 30 seconds in milliseconds
    private int socketTimeout = 60000; // 60 seconds in milliseconds
    private long connectionTimeToLive = 300000; // 300 seconds in milliseconds

    public int config(OConfigItem config) {
        for (OConfigItem child : config.getChildren()) {
            switch (child.getKey()) {
                case "connectionRequestTimeout":
                case "ConnectionRequestTimeout":
                    connectionRequestTimeout = Integer.parseInt(child.getValues().get(0).getString());
                    break;
                case "connectTimeout":
                case "ConnectTimeout":
                    connectTimeout = Integer.parseInt(child.getValues().get(0).getString());
                    break;
                case "socketTimeout":
                case "SocketTimeout":
                    socketTimeout = Integer.parseInt(child.getValues().get(0).getString());
                    break;
                case "connectionTimeToLive":
                case "ConnectionTimeToLive":
                    connectionTimeToLive = Long.parseLong(child.getValues().get(0).getString());
                    break;
                default:
            }
        }
        return 0;
    }

    @Override
    public PooledObject<CloseableHttpClient> makeObject() throws Exception {
        RequestConfig requestConfig = RequestConfig.custom().setConnectionRequestTimeout(connectionRequestTimeout).setConnectTimeout(connectTimeout)
                        .setSocketTimeout(socketTimeout).build();
        HttpClientBuilder builder = HttpClientBuilder.create();
        builder.setDefaultRequestConfig(requestConfig);
        builder.setConnectionTimeToLive(connectionTimeToLive, TimeUnit.MILLISECONDS);
        return new DefaultPooledObject<>(builder.build());
    }

    @Override
    public void destroyObject(PooledObject<CloseableHttpClient> pooledObject) throws Exception {
        try {
            pooledObject.getObject().close();
        } catch (Exception e) {
            // do nothing
        } finally {
            pooledObject.invalidate();
            ;
        }
    }

    @Override
    public boolean validateObject(PooledObject<CloseableHttpClient> pooledObject) {
        return pooledObject.getObject() != null;
    }

    @Override
    public void activateObject(PooledObject<CloseableHttpClient> pooledObject) throws Exception {
        // do nothing
    }

    @Override
    public void passivateObject(PooledObject<CloseableHttpClient> pooledObject) throws Exception {
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

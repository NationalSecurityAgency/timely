package timely.collectd.plugin;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class PooledCloseableHttpClientFactory implements PooledObjectFactory {

    @Override
    public PooledObject makeObject() throws Exception {
        return new DefaultPooledObject(HttpClients.createDefault());
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
}

package timely.balancer.connection.tcp;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.tcp.TcpClient;

import java.io.IOException;

public class TcpClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost, TcpClient> {

    public TcpClientFactory() {

    }

    @Override
    public PooledObject<TcpClient> makeObject(TimelyBalancedHost k) throws Exception {
        return new DefaultPooledObject<>(new TcpClient(k.getHost(), k.getTcpPort()));
    }

    @Override
    public void destroyObject(TimelyBalancedHost k, PooledObject<TcpClient> o) throws Exception {
        try {
            o.getObject().flush();
            o.getObject().close();
        } catch (IOException e) {
            throw new IOException("Error closing connection to " + k.getHost() + ":" + k.getTcpPort());
        }
    }

    @Override
    public boolean validateObject(TimelyBalancedHost k, PooledObject<TcpClient> o) {
        if (k.isUp()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void activateObject(TimelyBalancedHost k, PooledObject<TcpClient> o) throws Exception {
        try {
            o.getObject().open();
        } catch (IOException e) {
            throw new IOException("Unable to connect to " + k.getHost() + ":" + k.getTcpPort());
        }
    }

    @Override
    public void passivateObject(TimelyBalancedHost k, PooledObject<TcpClient> o) throws Exception {
        try {
            o.getObject().flush();
        } catch (Exception e) {
            throw new IOException("Error flushing connection to " + k.getHost() + ":" + k.getTcpPort());
        }
    }
}

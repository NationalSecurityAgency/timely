package timely.balancer.connection.udp;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.udp.UdpClient;

import java.io.IOException;

public class UdpClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost, UdpClient> {

    public UdpClientFactory() {
    }

    @Override
    public PooledObject<UdpClient> makeObject(TimelyBalancedHost k) throws Exception {
        return new DefaultPooledObject<>(new UdpClient(k.getHost(), k.getUdpPort()));
    }

    @Override
    public void destroyObject(TimelyBalancedHost k, PooledObject<UdpClient> o) throws Exception {
        try {
            o.getObject().flush();
            o.getObject().close();
        } catch (IOException e) {
            throw new IOException("Error closing connection to " + k.getHost() + ":" + k.getUdpPort());
        }
    }

    @Override
    public boolean validateObject(TimelyBalancedHost k, PooledObject<UdpClient> o) {
        if (k.isUp()) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void activateObject(TimelyBalancedHost k, PooledObject<UdpClient> o) throws Exception {
        try {
            o.getObject().open();
        } catch (IOException e) {
            throw new IOException("Unable to connect to " + k.getHost() + ":" + k.getUdpPort());
        }
    }

    @Override
    public void passivateObject(TimelyBalancedHost k, PooledObject<UdpClient> o) throws Exception {
        try {
            o.getObject().flush();
        } catch (Exception e) {
            throw new IOException("Error flushing connection to " + k.getHost() + ":" + k.getUdpPort());
        }
    }
}

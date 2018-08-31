package timely.balancer.connection.udp;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.udp.UdpClient;

public class UdpClientFactory implements KeyedPooledObjectFactory<TimelyBalancedHost, UdpClient> {

    public UdpClientFactory() {
    }

    @Override
    public PooledObject<UdpClient> makeObject(TimelyBalancedHost k) throws Exception {
        return new DefaultPooledObject<>(new UdpClient(k.getHost(), k.getUdpPort()));
    }

    @Override
    public void destroyObject(TimelyBalancedHost k, PooledObject<UdpClient> o) throws Exception {

    }

    @Override
    public boolean validateObject(TimelyBalancedHost k, PooledObject<UdpClient> o) {
        return true;
    }

    @Override
    public void activateObject(TimelyBalancedHost timelyBalancedHost, PooledObject<UdpClient> o) throws Exception {
        o.getObject().open();
    }

    @Override
    public void passivateObject(TimelyBalancedHost timelyBalancedHost, PooledObject<UdpClient> o) throws Exception {
        o.getObject().close();
    }
}

package timely.balancer.connection.udp;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.udp.UdpClient;

public class UdpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost, UdpClient> {

    public UdpClientPool(BalancerConfiguration config) {
        super(new UdpClientFactory(), config.getServer().getUdpClientPool());
    }
}

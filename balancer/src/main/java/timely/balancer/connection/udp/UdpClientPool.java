package timely.balancer.connection.udp;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import timely.balancer.configuration.BalancerServerProperties;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.udp.UdpClient;

public class UdpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost,UdpClient> {

    public UdpClientPool(BalancerServerProperties serverProperties) {
        super(new UdpClientFactory(), serverProperties.getUdpClientPool());
    }
}

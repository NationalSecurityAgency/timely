package timely.balancer.connection.tcp;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.tcp.TcpClient;

public class TcpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost,TcpClient> {

    public TcpClientPool(BalancerConfiguration config) {
        super(new TcpClientFactory(config.getServer().getTcpBufferSize()), config.getServer().getTcpClientPool());
    }
}

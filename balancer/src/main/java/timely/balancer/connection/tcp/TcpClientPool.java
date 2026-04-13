package timely.balancer.connection.tcp;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import timely.balancer.configuration.BalancerServerProperties;
import timely.balancer.connection.TimelyBalancedHost;
import timely.client.tcp.TcpClient;

public class TcpClientPool extends GenericKeyedObjectPool<TimelyBalancedHost,TcpClient> {

    public TcpClientPool(BalancerServerProperties config) {
        super(new TcpClientFactory(config.getTcpBufferSize()), config.getTcpClientPool(), config.getTcpClientPool().getAbandonedConfig());
    }
}

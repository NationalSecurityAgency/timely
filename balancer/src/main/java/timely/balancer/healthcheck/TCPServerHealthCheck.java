package timely.balancer.healthcheck;

import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.tcp.TcpClientPool;
import timely.client.tcp.TcpClient;

public class TCPServerHealthCheck implements ServerHealthCheck {

    final private TcpClientPool tcpClientPool;

    public TCPServerHealthCheck(TcpClientPool tcpClientPool) {
        this.tcpClientPool = tcpClientPool;
    }

    @Override
    public boolean isServerHealthy(TimelyBalancedHost timelyBalancedHost) {

        boolean healthy = true;
        TcpClient client = null;
        try {
            client = tcpClientPool.borrowObject(timelyBalancedHost);
            client.open();
        } catch (Exception e) {
            healthy = false;

        } finally {
            if (client != null) {
                tcpClientPool.returnObject(timelyBalancedHost, client);
            }
        }
        return healthy;
    }
}

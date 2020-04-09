package timely.balancer.resolver;

import timely.balancer.connection.TimelyBalancedHost;

public interface MetricResolver {

    public TimelyBalancedHost getHostPortKeyIngest(String metric);

    public TimelyBalancedHost getHostPortKey(String metric);

    public void close() throws Exception;
}

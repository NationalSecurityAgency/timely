package timely.balancer.resolver;

import timely.balancer.connection.TimelyBalancedHost;

public interface MetricResolver {

    public TimelyBalancedHost getHostPortKey(String metric);
}

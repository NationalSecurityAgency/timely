package timely.balancer.healthcheck;

import timely.balancer.connection.TimelyBalancedHost;

public interface ServerHealthCheck {

    public boolean isServerHealthy(TimelyBalancedHost timelyBalancedHost);
}

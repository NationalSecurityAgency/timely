package timely.balancer.healthcheck;

import java.util.List;

import timely.balancer.connection.TimelyBalancedHost;

public interface HealthChecker {

    public void setTimelyHosts(List<TimelyBalancedHost> timelyHosts);

    public void close() throws Exception;
}

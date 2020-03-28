package timely.balancer.healthcheck;

import timely.balancer.connection.TimelyBalancedHost;

import java.util.List;

public interface HealthChecker {

    public void setTimelyHosts(List<TimelyBalancedHost> timelyHosts);

}

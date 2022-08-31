package timely.balancer.test;

import java.util.List;

import timely.balancer.HealthChecker;
import timely.balancer.configuration.BalancerProperties;
import timely.balancer.configuration.BalancerServerProperties;
import timely.balancer.connection.TimelyBalancedHost;

public class TestHealthChecker extends HealthChecker {

    public TestHealthChecker(BalancerProperties balancerProperties, BalancerServerProperties balancerServerProperties) {
        super(balancerProperties, balancerServerProperties);
    }

    public List<TimelyBalancedHost> getTimelyHosts() {
        return this.timelyHosts;
    }
}

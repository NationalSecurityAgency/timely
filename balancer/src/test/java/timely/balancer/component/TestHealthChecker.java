package timely.balancer.component;

import java.util.List;

import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;

import timely.balancer.configuration.BalancerProperties;
import timely.balancer.configuration.BalancerServerProperties;
import timely.balancer.connection.TimelyBalancedHost;

@Primary
@Component
public class TestHealthChecker extends HealthChecker {

    public TestHealthChecker(BalancerProperties balancerProperties, BalancerServerProperties balancerServerProperties) {
        super(balancerProperties, balancerServerProperties);
    }

    @Override
    public void setup() {

    }

    @Override
    public void close() {

    }

    public List<TimelyBalancedHost> getTimelyHosts() {
        return this.timelyHosts;
    }
}

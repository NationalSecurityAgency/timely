package timely.balancer.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({BalancerProperties.class, BalancerServerProperties.class, BalancerWebsocketProperties.class, BalancerHttpProperties.class})
public class BalancerConfiguration {

}

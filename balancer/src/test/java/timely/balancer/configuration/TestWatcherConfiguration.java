package timely.balancer.configuration;

import org.apache.accumulo.core.client.AccumuloClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import timely.balancer.test.BalancerTestRule;
import timely.common.configuration.MiniAccumuloProperties;
import timely.common.configuration.TimelyProperties;

@Configuration
@EnableConfigurationProperties({MiniAccumuloProperties.class, TimelyProperties.class})
public class TestWatcherConfiguration {

    @Bean
    @Scope("prototype")
    public BalancerTestRule timelyTestWatcherAccumulo(AccumuloClient accumuloClient, TimelyProperties timelyProperties) throws Exception {

        return new BalancerTestRule(timelyProperties, accumuloClient);
    }
}

package timely.balancer.configuration;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import timely.common.configuration.AccumuloProperties;

@Configuration
public class TestConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public AccumuloProperties accumuloProperties() {
        AccumuloProperties accumuloProperties = new AccumuloProperties();
        accumuloProperties.setInstanceName("default");
        accumuloProperties.setUsername("root");
        accumuloProperties.setPassword("secret");
        return accumuloProperties;
    }
}

package timely.test.configuration;

import org.apache.accumulo.core.client.AccumuloClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import timely.common.configuration.TimelyProperties;
import timely.test.TimelyTestRule;

@Configuration
@EnableConfigurationProperties({TimelyProperties.class})
public class TestWatcherConfiguration {

    @Bean
    @Scope("prototype")
    public TimelyTestRule timelyTestWatcherAccumulo(AccumuloClient accumuloClient, TimelyProperties timelyProperties) throws Exception {
        return new TimelyTestRule(timelyProperties, accumuloClient);
    }
}

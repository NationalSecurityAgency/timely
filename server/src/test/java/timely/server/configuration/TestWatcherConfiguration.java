package timely.server.configuration;

import org.apache.accumulo.core.client.AccumuloClient;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import timely.common.configuration.MiniAccumuloProperties;
import timely.common.configuration.TimelyProperties;
import timely.test.TimelyServerTestRule;

@Configuration
@EnableConfigurationProperties({MiniAccumuloProperties.class, TimelyProperties.class})
public class TestWatcherConfiguration {

    @Bean
    @Scope("prototype")
    public TimelyServerTestRule timelyTestWatcherAccumulo(AccumuloClient accumuloClient, TimelyProperties timelyProperties) throws Exception {
        return new TimelyServerTestRule(timelyProperties, accumuloClient);
    }
}

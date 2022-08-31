package timely.server.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import timely.common.configuration.AccumuloProperties;
import timely.common.configuration.MetaCacheProperties;
import timely.common.configuration.TimelyProperties;
import timely.common.configuration.ZookeeperProperties;
import timely.store.InternalMetrics;
import timely.store.MetaCache;

@Configuration
@EnableConfigurationProperties({TimelyProperties.class, ZookeeperProperties.class, AccumuloProperties.class, MetaCacheProperties.class})
public class TimelyConfiguration {

    @Bean
    InternalMetrics internalMetrics(TimelyProperties timelyProperties) {
        return new InternalMetrics(timelyProperties);
    }

    @Bean(destroyMethod = "close")
    MetaCache metaCache(TimelyProperties timelyProperties, ZookeeperProperties zookeeperProperties, AccumuloProperties accumuloProperties,
                    MetaCacheProperties metaCacheProperties) {
        return new MetaCache(timelyProperties, zookeeperProperties, accumuloProperties, metaCacheProperties);
    }
}

package timely.common.configuration;

import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import timely.util.Exclusions;

@Configuration
@EnableConfigurationProperties({AccumuloProperties.class, CacheProperties.class, CorsProperties.class, HttpProperties.class, MetaCacheProperties.class,
        RestClientProperties.class, ScanProperties.class, SecurityProperties.class, ServerProperties.class, SslClientProperties.class,
        SslServerProperties.class, TimelyProperties.class, WebsocketProperties.class, WriteProperties.class, ZookeeperProperties.class})

public class TimelyCommonConfiguration {

    @Bean
    public Exclusions exclusions(TimelyProperties timelyProperties) {
        Exclusions exclusions = new Exclusions();
        if (StringUtils.isNotBlank(timelyProperties.getFilteredMetricsFile())) {
            exclusions.setFilteredMetricsFile(timelyProperties.getFilteredMetricsFile());
        }
        if (StringUtils.isNotBlank(timelyProperties.getFilteredTagsFile())) {
            exclusions.setFilteredTagsFile(timelyProperties.getFilteredTagsFile());
        }
        return exclusions;
    }
}

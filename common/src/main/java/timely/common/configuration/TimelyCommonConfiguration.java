package timely.common.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({AccumuloProperties.class, CacheProperties.class, CorsProperties.class, HttpProperties.class, MetaCacheProperties.class,
        RestClientProperties.class, ScanProperties.class, SecurityProperties.class, ServerProperties.class, SslClientProperties.class,
        SslServerProperties.class, TimelyProperties.class, WebsocketProperties.class, WriteProperties.class, ZookeeperProperties.class})

public class TimelyCommonConfiguration {

}

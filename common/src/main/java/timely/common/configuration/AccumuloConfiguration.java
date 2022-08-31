package timely.common.configuration;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AccumuloProperties.class)
public class AccumuloConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public AccumuloClient accumuloClient(AccumuloProperties accumuloProperties, ZookeeperProperties zookeeperProperties) {
        return Accumulo.newClient().to(accumuloProperties.getInstanceName(), zookeeperProperties.getServers())
                        .as(accumuloProperties.getUsername(), accumuloProperties.getPassword()).build();
    }
}

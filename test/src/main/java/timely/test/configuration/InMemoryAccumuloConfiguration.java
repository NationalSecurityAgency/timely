package timely.test.configuration;

import javax.inject.Singleton;

import org.apache.accumulo.core.client.AccumuloClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import datawave.accumulo.inmemory.InMemoryAccumulo;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import timely.common.configuration.AccumuloProperties;

@Configuration
@ConditionalOnProperty(name = "timely.in-memory-accumulo.enabled", havingValue = "true")
@EnableConfigurationProperties({InMemoryAccumuloProperties.class, AccumuloProperties.class})
public class InMemoryAccumuloConfiguration {

    protected static final String ROOT_USER = "root";

    @Bean(name = "InMemoryAccumulo")
    @Singleton
    public InMemoryAccumulo inMemoryAccumulo() throws Exception {
        return new InMemoryAccumulo();
    }

    @Bean
    @Primary
    public AccumuloClient inMemoryAccumuloClient(InMemoryAccumulo instance) throws Exception {
        return new InMemoryAccumuloClient(ROOT_USER, instance);
    }
}

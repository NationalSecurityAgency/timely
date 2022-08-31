package timely.common.configuration;

import javax.inject.Singleton;

import org.apache.accumulo.core.client.AccumuloClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;

@Configuration
@ConditionalOnProperty(name = "timely.in-memory-accumulo.enabled", havingValue = "true")
@EnableConfigurationProperties({InMemoryAccumuloProperties.class, AccumuloProperties.class})
public class InMemoryAccumuloConfiguration {

    protected static final String ROOT_USER = "root";

    @Bean(name = "InMemoryInstance")
    @Singleton
    public InMemoryInstance inMemoryInstance() throws Exception {
        return new InMemoryInstance();
    }

    @Bean
    @Primary
    public AccumuloClient inMemoryAccumuloClient(InMemoryInstance instance) throws Exception {
        return new InMemoryAccumuloClient(ROOT_USER, instance);
    }
}

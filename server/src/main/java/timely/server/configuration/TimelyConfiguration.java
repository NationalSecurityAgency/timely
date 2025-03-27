package timely.server.configuration;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.netty.handler.ssl.SslContext;
import timely.common.component.AuthenticationService;
import timely.common.configuration.AccumuloProperties;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.CorsProperties;
import timely.common.configuration.HttpProperties;
import timely.common.configuration.MetaCacheProperties;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.ServerProperties;
import timely.common.configuration.SslServerProperties;
import timely.common.configuration.TimelyProperties;
import timely.common.configuration.WebsocketProperties;
import timely.common.configuration.ZookeeperProperties;
import timely.server.Server;
import timely.server.store.DataStore;
import timely.server.store.InternalMetrics;
import timely.server.store.MetaCache;
import timely.server.store.cache.DataStoreCache;
import timely.util.Exclusions;

@Configuration
@EnableConfigurationProperties({TimelyProperties.class, ZookeeperProperties.class, AccumuloProperties.class, MetaCacheProperties.class})
public class TimelyConfiguration {

    @Bean
    InternalMetrics internalMetrics(TimelyProperties timelyProperties) {
        return new InternalMetrics(timelyProperties);
    }

    @Bean(destroyMethod = "close")
    public MetaCache metaCache(TimelyProperties timelyProperties, ZookeeperProperties zookeeperProperties, AccumuloProperties accumuloProperties,
                    MetaCacheProperties metaCacheProperties) {
        return new MetaCache(timelyProperties, zookeeperProperties, accumuloProperties, metaCacheProperties);
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public Server server(ApplicationContext applicationContext, AccumuloClient accumuloClient, DataStore dataStore, DataStoreCache dataStoreCache,
                    @Qualifier("nettySslContext") SslContext sslContext, AuthenticationService authenticationService, CuratorFramework curatorFramework,
                    MetaCache metaCache, TimelyProperties timelyProperties, SecurityProperties securityProperties, ServerProperties serverProperties,
                    HttpProperties httpProperties, CorsProperties corsProperties, WebsocketProperties websocketProperties,
                    SslServerProperties sslServerProperties) {
        Server server = new Server(applicationContext, accumuloClient, dataStore, dataStoreCache, sslContext, authenticationService, curatorFramework,
                        metaCache, timelyProperties, securityProperties, serverProperties, httpProperties, corsProperties, websocketProperties,
                        sslServerProperties);
        server.start();
        return server;
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public DataStore dataStore(ApplicationContext applicationContext, AccumuloClient accumuloClient, DataStoreCache dataStoreCache,
                    AuthenticationService authenticationService, InternalMetrics internalMetrics, MetaCache metaCache, TimelyProperties timelyProperties,
                    ZookeeperProperties zookeeperProperties, AccumuloProperties accumuloProperties, SecurityProperties securityProperties,
                    CacheProperties cacheProperties, Exclusions exclusions) throws Exception {
        DataStore dataStore = new DataStore(applicationContext, accumuloClient, dataStoreCache, authenticationService, internalMetrics, metaCache,
                        timelyProperties, zookeeperProperties, accumuloProperties, securityProperties, cacheProperties, exclusions);
        dataStore.start();
        return dataStore;
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public DataStoreCache dataStoreCache(CuratorFramework curatorFramework, AuthenticationService authenticationService, InternalMetrics internalMetrics,
                    TimelyProperties timelyProperties, CacheProperties cacheProperties) throws Exception {
        DataStoreCache dataStoreCache = new DataStoreCache(curatorFramework, authenticationService, internalMetrics, timelyProperties, cacheProperties);
        dataStoreCache.start();
        return dataStoreCache;
    }
}

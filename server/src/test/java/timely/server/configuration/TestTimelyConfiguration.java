package timely.server.configuration;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import io.netty.handler.ssl.SslContext;
import timely.common.component.AuthenticationService;
import timely.common.configuration.AccumuloProperties;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.CorsProperties;
import timely.common.configuration.HttpProperties;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.ServerProperties;
import timely.common.configuration.SslServerProperties;
import timely.common.configuration.TimelyProperties;
import timely.common.configuration.WebsocketProperties;
import timely.common.configuration.ZookeeperProperties;
import timely.server.store.DataStore;
import timely.server.store.InternalMetrics;
import timely.server.store.MetaCache;
import timely.server.store.cache.DataStoreCache;
import timely.server.test.TestCaptureRequestHandler;
import timely.server.test.TestDataStore;
import timely.server.test.TestDataStoreCache;
import timely.server.test.TestServer;
import timely.test.configuration.MiniAccumuloProperties;
import timely.util.Exclusions;

@Configuration
@EnableConfigurationProperties({MiniAccumuloProperties.class, TabletMetadataProperties.class})
@ComponentScan(basePackages = {"timely.server", "timely.common", "timely.test"})
public class TestTimelyConfiguration {

    @Bean
    @Qualifier("http")
    public TestCaptureRequestHandler httpCaptureRequestHandler() {
        return new TestCaptureRequestHandler(false);
    }

    @Bean
    @Qualifier("tcp")
    public TestCaptureRequestHandler tcpCaptureRequestHandler() {
        return new TestCaptureRequestHandler(false);
    }

    @Bean
    @Qualifier("udp")
    public TestCaptureRequestHandler udpCaptureRequestHandler() {
        return new TestCaptureRequestHandler(false);
    }

    @Bean(destroyMethod = "shutdown")
    public TestServer testTimelyServer(ApplicationContext applicationContext, AccumuloClient accumuloClient, DataStore dataStore, DataStoreCache dataStoreCache,
                    @Qualifier("nettySslContext") SslContext sslContext, AuthenticationService authenticationService, CuratorFramework curatorFramework,
                    MetaCache metaCache, TimelyProperties timelyProperties, SecurityProperties securityProperties, ServerProperties serverProperties,
                    HttpProperties httpProperties, CorsProperties corsProperties, WebsocketProperties websocketProperties,
                    SslServerProperties sslServerProperties, @Qualifier("http") TestCaptureRequestHandler httpRequests,
                    @Qualifier("tcp") TestCaptureRequestHandler tcpRequests, @Qualifier("udp") TestCaptureRequestHandler udpRequests) {
        TestServer timelyServer = new TestServer(applicationContext, accumuloClient, dataStore, dataStoreCache, sslContext, authenticationService,
                        curatorFramework, metaCache, timelyProperties, securityProperties, serverProperties, httpProperties, corsProperties,
                        websocketProperties, sslServerProperties, httpRequests, tcpRequests, udpRequests);
        timelyServer.start();
        return timelyServer;
    }

    @Bean(destroyMethod = "shutdown")
    // ensure that either MiniAccumulo or InMemoryAccumulo are created first
    @DependsOn("CuratorFramework")
    public TestDataStore dataStore(ApplicationContext applicationContext, AccumuloClient accumuloClient, DataStoreCache dataStoreCache,
                    AuthenticationService authenticationService, InternalMetrics internalMetrics, MetaCache metaCache, TimelyProperties timelyProperties,
                    ZookeeperProperties zookeeperProperties, AccumuloProperties accumuloProperties, SecurityProperties securityProperties,
                    CacheProperties cacheProperties, Exclusions exclusions) throws Exception {
        TestDataStore dataStore = new TestDataStore(applicationContext, accumuloClient, dataStoreCache, authenticationService, internalMetrics, metaCache,
                        timelyProperties, zookeeperProperties, accumuloProperties, securityProperties, cacheProperties, exclusions);
        dataStore.start();
        return dataStore;
    }

    @Bean(destroyMethod = "shutdown")
    public TestDataStoreCache dataStoreCache(CuratorFramework curatorFramework, AuthenticationService authenticationService, InternalMetrics internalMetrics,
                    TimelyProperties timelyProperties, CacheProperties cacheProperties) throws Exception {
        TestDataStoreCache dataStoreCache = new TestDataStoreCache(curatorFramework, authenticationService, internalMetrics, timelyProperties, cacheProperties);
        dataStoreCache.start();
        return dataStoreCache;
    }
}

package timely.balancer.configuration;

import javax.net.ssl.SSLContext;

import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.netty.handler.ssl.SslContext;
import timely.balancer.Balancer;
import timely.balancer.HealthChecker;
import timely.balancer.MetricResolver;
import timely.common.component.AuthenticationService;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.SslClientProperties;
import timely.common.configuration.SslServerProperties;
import timely.common.configuration.ZookeeperProperties;

@Configuration
@EnableConfigurationProperties({BalancerProperties.class, BalancerServerProperties.class, BalancerWebsocketProperties.class, BalancerHttpProperties.class})
public class BalancerConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Balancer balancer(ApplicationContext applicationContext, @Qualifier("nettySslContext") SslContext serverSslContext,
                    @Qualifier("outboundJDKSslContext") SSLContext clientSSLContext, AuthenticationService authenticationService, MetricResolver metricResolver,
                    BalancerProperties balancerProperties, ZookeeperProperties zookeeperProperties, SecurityProperties securityProperties,
                    SslServerProperties sslServerProperties, SslClientProperties sslClientProperties, BalancerServerProperties balancerServerProperties,
                    BalancerHttpProperties balancerHttpProperties, BalancerWebsocketProperties balancerWebsocketProperties) {
        Balancer balancer = new Balancer(applicationContext, serverSslContext, clientSSLContext, authenticationService, metricResolver, balancerProperties,
                        zookeeperProperties, securityProperties, sslServerProperties, sslClientProperties, balancerServerProperties, balancerHttpProperties,
                        balancerWebsocketProperties);
        balancer.start();
        return balancer;
    }

    @Bean
    @ConditionalOnMissingBean
    public HealthChecker healthChecker(BalancerProperties balancerProperties, BalancerServerProperties balancerServerProperties) {
        HealthChecker healthChecker = new HealthChecker(balancerProperties, balancerServerProperties);
        healthChecker.start();
        return healthChecker;
    }

    @Bean
    @ConditionalOnMissingBean
    public MetricResolver metricResolver(CuratorFramework curatorFramework, BalancerProperties balancerProperties, CacheProperties cacheProperties,
                    HealthChecker healthChecker) throws Exception {
        MetricResolver metricResolver = new MetricResolver(curatorFramework, balancerProperties, cacheProperties, healthChecker);
        metricResolver.start();
        return metricResolver;
    }
}

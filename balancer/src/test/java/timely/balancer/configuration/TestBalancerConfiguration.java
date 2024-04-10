package timely.balancer.configuration;

import javax.net.ssl.SSLContext;

import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import io.netty.handler.ssl.SslContext;
import timely.balancer.HealthChecker;
import timely.balancer.MetricResolver;
import timely.balancer.test.TestBalancer;
import timely.balancer.test.TestHealthChecker;
import timely.balancer.test.TestMetricResolver;
import timely.common.component.AuthenticationService;
import timely.common.configuration.CacheProperties;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.SslClientProperties;
import timely.common.configuration.SslServerProperties;
import timely.common.configuration.ZookeeperProperties;

@Configuration
@ComponentScan(basePackages = {"timely.balancer", "timely.common", "timely.test"})
public class TestBalancerConfiguration {

    @Bean
    public TestBalancer balancer(ApplicationContext applicationContext, @Qualifier("nettySslContext") SslContext serverSslContext,
                    @Qualifier("outboundJDKSslContext") SSLContext clientSSLContext, AuthenticationService authenticationService, MetricResolver metricResolver,
                    BalancerProperties balancerProperties, ZookeeperProperties zookeeperProperties, SecurityProperties securityProperties,
                    SslServerProperties sslServerProperties, SslClientProperties sslClientProperties, BalancerServerProperties balancerServerProperties,
                    BalancerHttpProperties balancerHttpProperties, BalancerWebsocketProperties balancerWebsocketProperties) {
        TestBalancer balancer = new TestBalancer(applicationContext, serverSslContext, clientSSLContext, authenticationService, metricResolver,
                        balancerProperties, zookeeperProperties, securityProperties, sslServerProperties, sslClientProperties, balancerServerProperties,
                        balancerHttpProperties, balancerWebsocketProperties);
        balancer.start();
        return balancer;
    }

    @Bean
    public TestHealthChecker healthChecker(BalancerProperties balancerProperties, BalancerServerProperties balancerServerProperties) {
        TestHealthChecker healthChecker = new TestHealthChecker(balancerProperties, balancerServerProperties);
        healthChecker.start();
        return healthChecker;
    }

    @Bean
    public TestMetricResolver metricResolver(CuratorFramework curatorFramework, BalancerProperties balancerProperties, CacheProperties cacheProperties,
                    HealthChecker healthChecker) throws Exception {
        TestMetricResolver metricResolver = new TestMetricResolver(curatorFramework, balancerProperties, cacheProperties, healthChecker);
        metricResolver.start();
        return metricResolver;
    }
}

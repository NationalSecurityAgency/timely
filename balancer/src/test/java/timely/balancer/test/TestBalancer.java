package timely.balancer.test;

import javax.net.ssl.SSLContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import io.netty.handler.ssl.SslContext;
import timely.balancer.Balancer;
import timely.balancer.MetricResolver;
import timely.balancer.configuration.BalancerHttpProperties;
import timely.balancer.configuration.BalancerProperties;
import timely.balancer.configuration.BalancerServerProperties;
import timely.balancer.configuration.BalancerWebsocketProperties;
import timely.common.component.AuthenticationService;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.SslClientProperties;
import timely.common.configuration.SslServerProperties;
import timely.common.configuration.ZookeeperProperties;

public class TestBalancer extends Balancer {
    private static final Logger log = LoggerFactory.getLogger(TestBalancer.class);

    public TestBalancer(ApplicationContext applicationContext, SslContext serverSslContext, SSLContext clientSSLContext,
                    AuthenticationService authenticationService, MetricResolver metricResolver, BalancerProperties balancerProperties,
                    ZookeeperProperties zookeeperProperties, SecurityProperties securityProperties, SslServerProperties sslServerProperties,
                    SslClientProperties sslClientProperties, BalancerServerProperties balancerServerProperties, BalancerHttpProperties balancerHttpProperties,
                    BalancerWebsocketProperties balancerWebsocketProperties) {
        super(applicationContext, serverSslContext, clientSSLContext, authenticationService, metricResolver, balancerProperties, zookeeperProperties,
                        securityProperties, sslServerProperties, sslClientProperties, balancerServerProperties, balancerHttpProperties,
                        balancerWebsocketProperties);
        DEFAULT_EVENT_LOOP_THREADS = 1;
        log.info("Starting TestBalancer");
    }
}

package timely.common.configuration;

import static org.apache.accumulo.core.conf.ConfigurationTypeHelper.getTimeInMillis;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
@ConditionalOnExpression("!T(org.springframework.util.StringUtils).isEmpty('${timely.zookeeper.servers:}')")
@EnableConfigurationProperties({ZookeeperProperties.class})
public class CuratorConfiguration {

    @Bean(destroyMethod = "close")
    @Scope("singleton")
    @ConditionalOnMissingBean
    public CuratorFramework curatorFramework(ZookeeperProperties zookeeperProperties) {
        RetryPolicy retryPolicy = new RetryForever(1000);
        int timeout = Long.valueOf(getTimeInMillis(zookeeperProperties.getTimeout())).intValue();
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zookeeperProperties.getServers(), timeout, 10000, retryPolicy);
        curatorFramework.start();
        return curatorFramework;
    }
}

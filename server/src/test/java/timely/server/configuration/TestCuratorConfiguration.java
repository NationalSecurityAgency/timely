package timely.server.configuration;

import static org.apache.accumulo.core.conf.ConfigurationTypeHelper.getTimeInMillis;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import timely.common.configuration.ZookeeperProperties;

/*
 * The purpose of this class is to wait for the creation of either ZooKeeperServerMain (in-memory)
 * or MiniAccumuloCluster (mini accumulo) before creating a CuratorFramework so that the components
 * that need CuratorFramework autowired will wait.
 */
@Configuration
public class TestCuratorConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TestCuratorConfiguration.class);

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "timely.in-memory-accumulo.enabled", havingValue = "true")
    @DependsOn("ZooKeeperServerMain")
    public CuratorFramework inMemoryCuratorFramework(ZookeeperProperties zookeeperProperties) {
        RetryPolicy retryPolicy = new RetryForever(1000);
        int timeout = Long.valueOf(getTimeInMillis(zookeeperProperties.getTimeout())).intValue();
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zookeeperProperties.getServers(), timeout, 10000, retryPolicy);
        curatorFramework.start();
        return curatorFramework;
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "timely.mini-accumulo.enabled", havingValue = "true")
    @DependsOn("MiniAccumuloCluster")
    public CuratorFramework miniAccumuloCuratorFramework(ZookeeperProperties zookeeperProperties) {
        RetryPolicy retryPolicy = new RetryForever(1000);
        int timeout = Long.valueOf(getTimeInMillis(zookeeperProperties.getTimeout())).intValue();
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(zookeeperProperties.getServers(), timeout, 10000, retryPolicy);
        curatorFramework.start();
        return curatorFramework;
    }
}

package timely.balancer.test;

import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.balancer.HealthChecker;
import timely.balancer.MetricResolver;
import timely.balancer.configuration.BalancerProperties;
import timely.balancer.connection.TimelyBalancedHost;
import timely.common.configuration.CacheProperties;

public class TestMetricResolver extends MetricResolver {

    private static final Logger log = LoggerFactory.getLogger(TestMetricResolver.class);

    public TestMetricResolver(CuratorFramework curatorFramework, BalancerProperties balancerProperties, CacheProperties cacheProperties,
                    HealthChecker healthChecker) throws Exception {
        super(curatorFramework, balancerProperties, cacheProperties, healthChecker);
    }

    protected void createAssignmentFile() {
        try {
            log.info("Creating assignment file: " + balancerProperties.getAssignmentFile());
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(configuration);
            Path assignmentFile = new Path(balancerProperties.getAssignmentFile());
            fs.setWriteChecksum(false);
            fs.create(assignmentFile);
            fs.deleteOnExit(assignmentFile);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public List<TimelyBalancedHost> getServerList() {
        return this.serverList;
    }
}

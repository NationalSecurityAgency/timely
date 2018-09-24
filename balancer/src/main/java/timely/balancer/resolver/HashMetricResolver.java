package timely.balancer.resolver;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.healthcheck.HealthChecker;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class HashMetricResolver implements MetricResolver {

    private static final Logger LOG = LoggerFactory.getLogger(HashMetricResolver.class);

    private Map<Integer, TimelyBalancedHost> serverMap = new HashMap<>();
    private Random r = new Random();
    final private HealthChecker healthChecker;

    public HashMetricResolver(BalancerConfiguration config, HealthChecker healthChecker) {
        int n = 0;
        for (TimelyBalancedHost h : config.getTimelyHosts()) {
            h.setConfig(config);
            serverMap.put(n++, h);
        }
        this.healthChecker = healthChecker;
    }

    @Override
    public TimelyBalancedHost getHostPortKey(String metric) {
        int n;
        if (StringUtils.isBlank(metric)) {
            n = Math.abs(r.nextInt() & Integer.MAX_VALUE) % serverMap.size();
        } else {
            n = Math.abs(metric.hashCode() & Integer.MAX_VALUE) % serverMap.size();
        }
        TimelyBalancedHost hpk = serverMap.get(n);
        LOG.info("routing metric: " + metric + " to " + hpk.getHost() + ":" + hpk.getTcpPort());
        return hpk;

    }
}

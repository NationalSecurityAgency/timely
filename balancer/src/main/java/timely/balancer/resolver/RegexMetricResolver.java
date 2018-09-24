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

public class RegexMetricResolver implements MetricResolver {

    private static final Logger LOG = LoggerFactory.getLogger(RegexMetricResolver.class);

    private Map<Integer, TimelyBalancedHost> serverMap = new HashMap<>();
    private Random r = new Random();
    final private HealthChecker healthChecker;

    public RegexMetricResolver(BalancerConfiguration config, HealthChecker healthChecker) {
        int n = 0;
        for (TimelyBalancedHost h : config.getTimelyHosts()) {
            h.setConfig(config);
            serverMap.put(n++, h);
        }
        this.healthChecker = healthChecker;
    }

    @Override
    public TimelyBalancedHost getHostPortKey(String metric) {

        TimelyBalancedHost tbh = null;
        if (StringUtils.isNotBlank(metric)) {
            for (TimelyBalancedHost h : serverMap.values()) {
                if (metric.matches(h.getRegex()) && h.isUp()) {
                    tbh = h;
                    break;
                }
            }
        }
        if (tbh == null) {
            tbh = getFallbackHost(metric);
        }

        LOG.info("routing metric: " + metric + " to " + tbh.getHost() + ":" + tbh.getTcpPort());
        return tbh;

    }

    private TimelyBalancedHost getFallbackHost(String metric) {

        TimelyBalancedHost tbh;
        if (StringUtils.isBlank(metric)) {
            do {
                tbh = serverMap.get(Math.abs(r.nextInt() & Integer.MAX_VALUE) % serverMap.size());
            } while (!tbh.isUp());
        } else {
            // reverse metric so that similar metrics go to different hosts in a
            // reproducible way
            String reverseMetric = StringUtils.reverse(metric);
            int x = 0;
            do {
                // subtract a charter off of the end until we find a server that
                // is up
                int hash = reverseMetric.substring(x++).hashCode();
                tbh = serverMap.get(Math.abs(hash & Integer.MAX_VALUE) % serverMap.size());
            } while (!tbh.isUp());
        }

        // if all else fails
        if (!tbh.isUp()) {
            for (TimelyBalancedHost h : serverMap.values()) {
                if (h.isUp()) {
                    tbh = h;
                    break;
                }
            }
        }
        return tbh;
    }
}

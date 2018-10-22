package timely.balancer.resolver;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.ArrivalRate;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.healthcheck.HealthChecker;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;

public class BalancedMetricResolver implements MetricResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BalancedMetricResolver.class);

    private Map<String, TimelyBalancedHost> metricToHostMap = new HashMap<>();
    private Map<Integer, TimelyBalancedHost> serverMap = new HashMap<>();
    private Map<String, ArrivalRate> metricMap = new HashMap<>();
    private Random r = new Random();
    final private HealthChecker healthChecker;
    private Timer timer = new Timer("RebalanceTimer", true);
    private long balanceUntil = System.currentTimeMillis() + 600000l;

    public BalancedMetricResolver(BalancerConfiguration config, HealthChecker healthChecker) {
        int n = 0;
        synchronized (this) {
            for (TimelyBalancedHost h : config.getTimelyHosts()) {
                h.setConfig(config);
                serverMap.put(n++, h);
            }
        }
        this.healthChecker = healthChecker;
        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                if (System.currentTimeMillis() < balanceUntil) {
                    try {
                        balance();
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        }, 60000, 120000);
    }

    private TimelyBalancedHost getLeastUsedHost() {

        Map<Double, TimelyBalancedHost> rateSortedHosts = new TreeMap<>();
        for (Map.Entry<Integer, TimelyBalancedHost> e : serverMap.entrySet()) {
            rateSortedHosts.put(e.getValue().getArrivalRate(), e.getValue());
        }

        Iterator<Map.Entry<Double, TimelyBalancedHost>> itr = rateSortedHosts.entrySet().iterator();

        TimelyBalancedHost tbh = null;

        while (itr.hasNext() && tbh == null) {
            TimelyBalancedHost currentTBH = itr.next().getValue();
            if (currentTBH.isUp()) {
                tbh = currentTBH;
            }
        }
        return tbh;
    }

    private TimelyBalancedHost getRandomHost(TimelyBalancedHost notThisOne) {

        TimelyBalancedHost tbh = null;
        for (int x = 0; tbh == null && x < serverMap.size(); x++) {
            tbh = serverMap.get(Math.abs(r.nextInt() & Integer.MAX_VALUE) % serverMap.size());
            if (!tbh.isUp()) {
                tbh = null;
            } else if (notThisOne != null && tbh.equals(notThisOne)) {
                tbh = null;
            }
        }
        return tbh;
    }

    private TimelyBalancedHost getRoundRobinHost() {
        TimelyBalancedHost tbh;
        int x = metricToHostMap.size() % serverMap.size();
        tbh = serverMap.get(x);
        if (tbh.isUp()) {
            return tbh;
        } else {
            return getRandomHost(null);
        }
    }

    synchronized public void balance() {
        LOG.info("rebalancing begin");
        Map<Double, TimelyBalancedHost> ratedSortedHosts = new TreeMap<>();
        double totalArrivalRate = 0;
        for (Map.Entry<Integer, TimelyBalancedHost> e : serverMap.entrySet()) {
            ratedSortedHosts.put(e.getValue().getArrivalRate(), e.getValue());
            totalArrivalRate += e.getValue().getArrivalRate();
        }

        Iterator<Map.Entry<Double, TimelyBalancedHost>> itr = ratedSortedHosts.entrySet().iterator();
        TimelyBalancedHost mostUsed = null;
        TimelyBalancedHost leastUsed = null;

        while (itr.hasNext()) {
            TimelyBalancedHost currentTBH = itr.next().getValue();
            if (currentTBH.isUp()) {
                if (leastUsed == null) {
                    leastUsed = currentTBH;
                    mostUsed = currentTBH;
                } else {
                    // should end up with the last server that is up
                    mostUsed = currentTBH;
                }
            }
        }

        double averageArrivalRate = totalArrivalRate / serverMap.size();
        double highestArrivalRate = mostUsed.getArrivalRate();
        double lowestArrivalRate = leastUsed.getArrivalRate();

        LOG.info("rebalancing high:{} avg:{} low:{}", highestArrivalRate, averageArrivalRate, lowestArrivalRate);

        // 5% over average
        int numReassigned = 0;
        if (highestArrivalRate > averageArrivalRate * 1.05) {
            LOG.info("rebalancing: high > 5% higher than average");
            // sort metrics by rate
            Map<Double, String> rateSortedMetrics = new TreeMap<>();
            for (Map.Entry<String, ArrivalRate> e : metricMap.entrySet()) {
                rateSortedMetrics.put(e.getValue().getRate(), e.getKey());
            }

            double desiredDeltaHighest = (highestArrivalRate - averageArrivalRate) * 0.1;
            double desiredDeltaLowest = (averageArrivalRate - lowestArrivalRate) * 0.1;
            Iterator<Map.Entry<Double, String>> metricItr = rateSortedMetrics.entrySet().iterator();
            boolean done = false;
            while (!done && metricItr.hasNext() && numReassigned < 10) {
                Map.Entry<Double, String> current = metricItr.next();
                Double currentRate = current.getKey();
                String currentMetric = current.getValue();

                if (desiredDeltaHighest > 0) {
                    if (metricToHostMap.get(currentMetric).equals(mostUsed)) {
                        if (desiredDeltaHighest > 0) {
                            numReassigned++;
                            metricToHostMap.put(currentMetric, leastUsed);
                            LOG.info("rebalancing: reassigning metric {} from server {}:{} to {}:{}", currentMetric,
                                    mostUsed.getHost(), mostUsed.getTcpPort(), leastUsed.getHost(),
                                    leastUsed.getTcpPort());
                            desiredDeltaLowest -= currentRate;
                            desiredDeltaHighest -= currentRate;
                        } else {
                            TimelyBalancedHost tbh = getRandomHost(mostUsed);
                            if (tbh != null) {
                                numReassigned++;
                                metricToHostMap.put(currentMetric, tbh);
                                LOG.info("rebalancing: reassigning metric {} from server {}:{} to {}:{}",
                                        currentMetric, mostUsed.getHost(), mostUsed.getTcpPort(), tbh.getHost(),
                                        tbh.getTcpPort());
                                desiredDeltaHighest -= currentRate;
                            }
                        }
                    }
                } else {
                    done = true;
                }
            }
        }
        LOG.info("rebalancing end - reassigned {}", numReassigned);
    }

    @Override
    public TimelyBalancedHost getHostPortKeyIngest(String metric) {
        if (metric != null) {
            ArrivalRate rate;
            rate = metricMap.get(metric);
            if (rate == null) {
                rate = new ArrivalRate();
                metricMap.put(metric, rate);
            }
            rate.arrived();
        }

        TimelyBalancedHost tbh;
        if (StringUtils.isBlank(metric)) {
            tbh = getRandomHost(null);
        } else {
            tbh = metricToHostMap.get(metric);
            if (tbh == null) {
                tbh = getRoundRobinHost();
                metricToHostMap.put(metric, tbh);
            } else if (!tbh.isUp()) {
                tbh = getLeastUsedHost();
                metricToHostMap.put(metric, tbh);
            }
        }

        // if all else fails
        if (tbh == null || !tbh.isUp()) {
            for (TimelyBalancedHost h : serverMap.values()) {
                if (h.isUp()) {
                    tbh = h;
                    break;
                }
            }
            if (tbh != null && StringUtils.isNotBlank(metric)) {
                metricToHostMap.put(metric, tbh);
            }
        }
        if (tbh != null) {
            tbh.arrived();
            tbh.calculateRate();
        }
        return tbh;
    }

    @Override
    public TimelyBalancedHost getHostPortKey(String metric) {
        TimelyBalancedHost tbh = null;
        if (StringUtils.isNotBlank(metric)) {
            tbh = metricToHostMap.get(metric);
        }

        if (tbh == null || !tbh.isUp()) {
            for (int x = 0; tbh == null && x < serverMap.size(); x++) {
                tbh = serverMap.get(Math.abs(r.nextInt() & Integer.MAX_VALUE) % serverMap.size());
                if (!tbh.isUp()) {
                    tbh = null;
                }
            }
        }

        // if all else fails
        if (tbh == null || !tbh.isUp()) {
            for (TimelyBalancedHost h : serverMap.values()) {
                if (h.isUp()) {
                    tbh = h;
                    break;
                }
            }
            if (tbh != null && StringUtils.isNotBlank(metric)) {
                metricToHostMap.put(metric, tbh);
            }
        }
        return tbh;
    }
}

package timely.balancer.resolver;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.locks.StampedLock;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.ArrivalRate;
import timely.balancer.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.healthcheck.HealthChecker;

public class BalancedMetricResolver implements MetricResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BalancedMetricResolver.class);

    private Map<String, TimelyBalancedHost> metricToHostMap = new TreeMap<>();
    private StampedLock metricToHostMapLock = new StampedLock();
    private Map<String, ArrivalRate> metricMap = new HashMap<>();
    private StampedLock metricMapLock = new StampedLock();
    private Map<Integer, TimelyBalancedHost> serverMap = new HashMap<>();
    private Random r = new Random();
    final private HealthChecker healthChecker;
    private Timer timer = new Timer("RebalanceTimer", true);
    private long balanceUntil = System.currentTimeMillis() + 1800000l;
    private int roundRobinCounter = 0;

    public BalancedMetricResolver(BalancerConfiguration config, HealthChecker healthChecker) {
        int n = 0;
        for (TimelyBalancedHost h : config.getTimelyHosts()) {
            h.setConfig(config);
            serverMap.put(n++, h);
        }
        long stamp = metricToHostMapLock.writeLock();
        try {
            metricToHostMap.putAll(readAssignments(config.getAssignmentFile()));
        } finally {
            metricToHostMapLock.unlockWrite(stamp);
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
        }, 600000, 120000);

        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    writeAssigments(config.getAssignmentFile());
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }, 300000, 3600000);
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
        try {
            int x = roundRobinCounter % serverMap.size();
            tbh = serverMap.get(x);
        } finally {
            roundRobinCounter++;
            if (roundRobinCounter == Integer.MAX_VALUE) {
                roundRobinCounter = 0;
            }
        }
        if (tbh.isUp()) {
            return tbh;
        } else {
            return getRandomHost(null);
        }
    }

    public void balance() {
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
        // double lowestArrivalRate = leastUsed.getArrivalRate();

        LOG.info("rebalancing high:{} avg:{}", highestArrivalRate, averageArrivalRate);

        // 5% over average
        int numReassigned = 0;
        if (highestArrivalRate > averageArrivalRate * 1.05) {
            long stamp1 = metricMapLock.readLock();
            long stamp2 = metricToHostMapLock.readLock();
            try {
                LOG.info("rebalancing: high > 5% higher than average");
                // sort metrics by rate
                Map<Double, String> rateSortedMetrics = new TreeMap<>();
                for (Map.Entry<String, ArrivalRate> e : metricMap.entrySet()) {
                    rateSortedMetrics.put(e.getValue().getRate(), e.getKey());
                }

                double desiredDeltaHighest = (highestArrivalRate - averageArrivalRate) * 0.1;
                Iterator<Map.Entry<Double, String>> metricItr = rateSortedMetrics.entrySet().iterator();
                boolean done = false;
                LOG.info("rebalancing: desiredDeltaHighest:{} rateSortedMetrics.size():{}", desiredDeltaHighest,
                        rateSortedMetrics.size());
                // advance to halfway
                for (int numMetric = 0; metricItr.hasNext() && numMetric <= rateSortedMetrics.size() / 2; numMetric++) {
                    metricItr.next();
                }
                long maxToReassign = Math.round(((double) metricMap.size() / serverMap.size()) * 0.20);
                while (!done && metricItr.hasNext() && numReassigned < maxToReassign) {
                    Map.Entry<Double, String> current = metricItr.next();
                    Double currentRate = current.getKey();
                    String currentMetric = current.getValue();

                    if (desiredDeltaHighest > 0) {
                        if (metricToHostMap.get(currentMetric).equals(mostUsed)) {
                            LOG.debug("rebalancing: trying to reassign metric {} from server {}:{}", currentMetric,
                                    mostUsed.getHost(), mostUsed.getTcpPort());
                            if (desiredDeltaHighest > 0) {
                                numReassigned++;
                                TimelyBalancedHost newHost = getRoundRobinHost();
                                metricToHostMap.put(currentMetric, newHost);
                                LOG.debug("rebalancing: reassigning metric {} from server {}:{} to {}:{}",
                                        currentMetric, mostUsed.getHost(), mostUsed.getTcpPort(), newHost.getHost(),
                                        newHost.getTcpPort());
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
                                } else {
                                    LOG.debug(
                                            "rebalancing: unable to reassign metric {} from server {}:{} - getRandomHost returned null",
                                            currentMetric, mostUsed.getHost(), mostUsed.getTcpPort());
                                }
                            }
                        }
                    } else {
                        done = true;
                    }
                }
            } finally {
                metricToHostMapLock.unlockRead(stamp2);
                metricMapLock.unlockRead(stamp1);
            }
        }
        LOG.info("rebalancing end - reassigned {}", numReassigned);
    }

    @Override
    public TimelyBalancedHost getHostPortKeyIngest(String metric) {
        if (StringUtils.isNotBlank(metric)) {
            ArrivalRate rate;
            long stamp = metricMapLock.readLock();
            try {
                rate = metricMap.get(metric);
                if (rate == null) {
                    rate = new ArrivalRate();
                    long writeStamp = metricMapLock.tryConvertToWriteLock(stamp);
                    if (writeStamp == 0) {
                        metricMapLock.unlockRead(stamp);
                        stamp = metricMapLock.writeLock();
                    } else {
                        stamp = writeStamp;
                    }
                    metricMap.put(metric, rate);
                }
            } finally {
                metricMapLock.unlock(stamp);
            }
            rate.arrived();
        }

        TimelyBalancedHost tbh;
        if (StringUtils.isBlank(metric)) {
            tbh = getRandomHost(null);
        } else {
            long stamp = metricToHostMapLock.readLock();
            try {
                tbh = metricToHostMap.get(metric);
                if (tbh == null) {
                    tbh = getRoundRobinHost();
                    long writeStamp = metricToHostMapLock.tryConvertToWriteLock(stamp);
                    if (writeStamp == 0) {
                        metricToHostMapLock.unlockRead(stamp);
                        stamp = metricToHostMapLock.writeLock();
                    } else {
                        stamp = writeStamp;
                    }
                    metricToHostMap.put(metric, tbh);
                } else if (!tbh.isUp()) {
                    TimelyBalancedHost oldTbh = tbh;
                    tbh = getLeastUsedHost();
                    LOG.debug("rebalancing from host that is down: reassigning metric {} from server {}:{} to {}:{}",
                            metric, oldTbh.getHost(), oldTbh.getTcpPort(), tbh.getHost(), tbh.getTcpPort());
                    long writeStamp = metricToHostMapLock.tryConvertToWriteLock(stamp);
                    if (writeStamp == 0) {
                        metricToHostMapLock.unlockRead(stamp);
                        stamp = metricToHostMapLock.writeLock();
                    } else {
                        stamp = writeStamp;
                    }
                    metricToHostMap.put(metric, tbh);
                }
            } finally {
                metricToHostMapLock.unlock(stamp);
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
                long stamp = metricToHostMapLock.writeLock();
                try {
                    metricToHostMap.put(metric, tbh);
                } finally {
                    metricToHostMapLock.unlockWrite(stamp);
                }
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
            long stamp = metricToHostMapLock.readLock();
            try {
                tbh = metricToHostMap.get(metric);
            } finally {
                metricToHostMapLock.unlockRead(stamp);
            }
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
                long stamp = metricToHostMapLock.writeLock();
                try {
                    metricToHostMap.put(metric, tbh);
                } finally {
                    metricToHostMapLock.unlockWrite(stamp);
                }
            }
        }
        return tbh;
    }

    private TimelyBalancedHost findHost(String host, int tcpPort) {
        TimelyBalancedHost tbh = null;
        for (TimelyBalancedHost h : serverMap.values()) {
            if (h.getHost().equals(host) && h.getTcpPort() == tcpPort) {
                tbh = h;
                break;
            }
        }
        return tbh;
    }

    private Map<String, TimelyBalancedHost> readAssignments(String path) {

        Map<String, TimelyBalancedHost> assignedMetricToHostMap = new TreeMap<>();
        CsvReader reader = null;
        try {
            reader = new CsvReader(new FileInputStream(path), ',', Charset.forName("UTF-8"));
            // reader.setSkipEmptyRecords(true);
            reader.setUseTextQualifier(false);

            // skip the headers
            boolean success = true;
            success = reader.readHeaders();

            while (success) {
                success = reader.readRecord();
                String[] nextLine = reader.getValues();
                if (nextLine.length > 3) {
                    String metric = nextLine[0];
                    String host = nextLine[1];
                    int tcpPort = Integer.parseInt(nextLine[2]);
                    TimelyBalancedHost tbh = findHost(host, tcpPort);
                    if (tbh == null) {
                        tbh = getRoundRobinHost();
                    } else {
                        LOG.trace("Found assigment: {} to {}:{}", metric, host, tcpPort);
                    }
                    assignedMetricToHostMap.put(metric, tbh);
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
        return assignedMetricToHostMap;
    }

    private void writeAssigments(String path) {

        CsvWriter writer = null;
        try {
            writer = new CsvWriter(new FileOutputStream(path), ',', Charset.forName("UTF-8"));
            writer.setUseTextQualifier(false);

            writer.write("metric");
            writer.write("host");
            writer.write("tcpPort");
            writer.write("rate");
            writer.endRecord();

            long stamp1 = metricMapLock.readLock();
            long stamp2 = metricToHostMapLock.readLock();
            try {
                for (Map.Entry<String, TimelyBalancedHost> e : metricToHostMap.entrySet()) {
                    writer.write(e.getKey());
                    writer.write(e.getValue().getHost());
                    writer.write(Integer.toString(e.getValue().getTcpPort()));
                    writer.write(Double.toString(metricMap.get(e.getKey()).getRate()));
                    writer.endRecord();
                    LOG.trace("Saving assigment: {} to {}:{}", e.getKey(), e.getValue().getHost(),
                            e.getValue().getTcpPort());
                }
            } finally {
                metricToHostMapLock.unlockRead(stamp2);
                metricMapLock.unlockRead(stamp1);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}

package timely.balancer.resolver;

import static timely.Server.SERVICE_DISCOVERY_PATH;
import static timely.balancer.Balancer.ASSIGNMENTS_LAST_UPDATED_PATH;
import static timely.balancer.Balancer.ASSIGNMENTS_LOCK_PATH;
import static timely.balancer.Balancer.LEADER_LATCH_PATH;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.ServerDetails;
import timely.balancer.ArrivalRate;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.healthcheck.HealthChecker;

public class BalancedMetricResolver implements MetricResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BalancedMetricResolver.class);
    private Map<String, TimelyBalancedHost> metricToHostMap = new TreeMap<>();
    private Map<String, ArrivalRate> metricMap = new HashMap<>();
    private ReentrantReadWriteLock balancerLock = new ReentrantReadWriteLock();
    private List<TimelyBalancedHost> serverList = new ArrayList<>();
    private Random r = new Random();
    final private HealthChecker healthChecker;
    private Timer timer = new Timer("RebalanceTimer", true);
    private Timer arrivalRateTimer = new Timer("AriivalRateTimerResolver", true);
    private int roundRobinCounter = 0;
    private Set<String> nonCachedMetrics = new HashSet<>();
    private ReentrantReadWriteLock nonCachedMetricsLock = new ReentrantReadWriteLock();
    private BalancerConfiguration balancerConfig;
    private CuratorFramework curatorFramework;
    private RetryPolicy retryPolicy = new RetryUntilElapsed(60000, 1000);
    private LeaderLatch leaderLatch;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private InterProcessReadWriteLock assignmentsLock;
    private DistributedAtomicLong assignmentsLastUpdatedInHdfs;
    private AtomicLong assignmentsLastUpdatedLocal = new AtomicLong(0);

    private String[] zkPaths = new String[] { LEADER_LATCH_PATH, ASSIGNMENTS_LAST_UPDATED_PATH, ASSIGNMENTS_LOCK_PATH,
            SERVICE_DISCOVERY_PATH };

    private enum BalanceType {
        HIGH_LOW, HIGH_AVG, AVG_LOW;
    }

    public BalancedMetricResolver(BalancerConfiguration balancerConfig, HealthChecker healthChecker) {
        this.balancerConfig = balancerConfig;
        this.healthChecker = healthChecker;

        // start curator framework
        curatorFramework = CuratorFrameworkFactory.newClient(balancerConfig.getZooKeeper().getServers(), 30000, 1000,
                retryPolicy);
        curatorFramework.start();
        ensureZkPaths(curatorFramework, zkPaths);
        assignmentsLock = new InterProcessReadWriteLock(curatorFramework, ASSIGNMENTS_LOCK_PATH);
        startLeaderLatch(curatorFramework);
        startServiceListener(curatorFramework);
        assignmentsLastUpdatedInHdfs = new DistributedAtomicLong(curatorFramework, ASSIGNMENTS_LAST_UPDATED_PATH,
                retryPolicy);

        TreeCacheListener listener = new TreeCacheListener() {

            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                LOG.info("Handling event {}. assignmentsLastUpdatedInHdfs:{}", event.getType().toString(),
                        new Date(assignmentsLastUpdatedInHdfs.get().postValue()));
                if (event.getType().equals(TreeCacheEvent.Type.NODE_UPDATED)) {
                    long lastLocalUpdate = assignmentsLastUpdatedLocal.get();
                    long lastHdfsUpdate = assignmentsLastUpdatedInHdfs.get().postValue();
                    if (lastHdfsUpdate > lastLocalUpdate) {
                        LOG.info("Reading assignments from hdfs lastHdfsUpdate ({}) > lastLocalUpdate ({})",
                                new Date(lastHdfsUpdate), new Date(lastLocalUpdate));
                        readAssignmentsFromHdfs();
                    } else {
                        LOG.info("Not reading assignments from hdfs lastHdfsUpdate ({}) <= lastLocalUpdate ({})",
                                new Date(lastHdfsUpdate), new Date(lastLocalUpdate));
                    }
                }
            }
        };

        try {
            TreeCache treeCache = new TreeCache(curatorFramework, ASSIGNMENTS_LAST_UPDATED_PATH);
            treeCache.getListenable().addListener(listener);
            treeCache.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        nonCachedMetricsLock.writeLock().lock();
        try {
            nonCachedMetrics.addAll(balancerConfig.getCache().getNonCachedMetrics());
        } finally {
            nonCachedMetricsLock.writeLock().unlock();
        }

        readAssignmentsFromHdfs();

        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                if (isLeader.get()) {
                    try {
                        balance();
                    } catch (Exception e) {
                        LOG.error(e.getMessage(), e);
                    }
                }
            }
        }, 900000, 900000);

        timer.schedule(new TimerTask() {

            @Override
            public void run() {
                try {
                    if (isLeader.get()) {
                        long lastLocalUpdate = assignmentsLastUpdatedLocal.get();
                        long lastHdfsUpdate = assignmentsLastUpdatedInHdfs.get().postValue();
                        if (lastLocalUpdate > lastHdfsUpdate) {
                            LOG.info("Writing assignments to hdfs lastLocalUpdate ({}) > lastHdfsUpdate ({})",
                                    new Date(lastLocalUpdate), new Date(lastHdfsUpdate));
                            writeAssigmentsToHdfs();
                        }
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }, 10000, 60000);
    }

    private void ensureZkPaths(CuratorFramework curatorFramework, String[] paths) {
        for (String s : paths) {
            try {
                Stat stat = curatorFramework.checkExists().forPath(s);
                if (stat == null) {
                    curatorFramework.create().creatingParentContainersIfNeeded().forPath(s);
                }
            } catch (Exception e) {
                LOG.info(e.getMessage());
            }

        }
    }

    private void startLeaderLatch(CuratorFramework curatorFramework) {
        try {

            this.leaderLatch = new LeaderLatch(curatorFramework, LEADER_LATCH_PATH);
            this.leaderLatch.start();
            this.leaderLatch.addListener(new LeaderLatchListener() {

                @Override
                public void isLeader() {
                    LOG.info("this balancer is the leader");
                    isLeader.set(true);
                    writeAssigmentsToHdfs();
                }

                @Override
                public void notLeader() {
                    LOG.info("this balancer is not the leader");
                    isLeader.set(false);
                }
            });
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void startServiceListener(CuratorFramework curatorFramework) {
        try {
            ServiceDiscovery discovery = ServiceDiscoveryBuilder.builder(ServerDetails.class).client(curatorFramework)
                    .basePath(SERVICE_DISCOVERY_PATH).build();
            discovery.start();
            Collection<ServiceInstance> instances = discovery.queryForInstances("timely-server");

            balancerLock.writeLock().lock();
            try {
                for (ServiceInstance si : instances) {
                    ServerDetails pl = (ServerDetails) si.getPayload();
                    TimelyBalancedHost tbh = TimelyBalancedHost.of(pl.getHost(), pl.getTcpPort(), pl.getHttpPort(),
                            pl.getWsPort(), pl.getUdpPort());
                    LOG.info("adding service {} host:{} tcpPort:{} httpPort:{} wsPort:{} udpPort:{}", si.getId(),
                            pl.getHost(), pl.getTcpPort(), pl.getHttpPort(), pl.getWsPort(), pl.getUdpPort());
                    tbh.setBalancerConfig(balancerConfig);
                    serverList.add(tbh);
                }
                healthChecker.setTimelyHosts(serverList);
            } finally {
                balancerLock.writeLock().unlock();
            }

            final ServiceCache serviceCache = discovery.serviceCacheBuilder().name("timely-server").build();
            ServiceCacheListener listener = new ServiceCacheListener() {

                @Override
                public void cacheChanged() {
                    boolean rebalanceNeeded = false;
                    balancerLock.writeLock().lock();
                    try {
                        List<ServiceInstance> instances = serviceCache.getInstances();
                        Set<TimelyBalancedHost> availableHosts = new HashSet<>();
                        for (ServiceInstance si : instances) {
                            ServerDetails pl = (ServerDetails) si.getPayload();
                            TimelyBalancedHost tbh = TimelyBalancedHost.of(pl.getHost(), pl.getTcpPort(),
                                    pl.getHttpPort(), pl.getWsPort(), pl.getUdpPort());
                            tbh.setBalancerConfig(balancerConfig);
                            availableHosts.add(tbh);
                        }

                        List<String> reassignMetrics = new ArrayList<>();
                        // remove hosts that are no longer available
                        Iterator<TimelyBalancedHost> itr = serverList.iterator();
                        while (itr.hasNext()) {
                            TimelyBalancedHost h = itr.next();
                            if (availableHosts.contains(h)) {
                                availableHosts.remove(h);
                            } else {
                                itr.remove();
                                LOG.info("removing service {}:{} host:{} tcpPort:{} httpPort:{} wsPort:{} udpPort:{}",
                                        h.getHost(), h.getTcpPort(), h.getHost(), h.getTcpPort(), h.getHttpPort(),
                                        h.getWsPort(), h.getUdpPort());
                                for (Map.Entry<String, TimelyBalancedHost> e : metricToHostMap.entrySet()) {
                                    if (e.getValue().equals(h)) {
                                        reassignMetrics.add(e.getKey());
                                    }
                                }
                                rebalanceNeeded = true;
                            }
                        }

                        // add new hosts that were not previously known
                        for (TimelyBalancedHost h : availableHosts) {
                            LOG.info("adding service {}:{} host:{} tcpPort:{} httpPort:{} wsPort:{} udpPort:{}",
                                    h.getHost(), h.getTcpPort(), h.getHost(), h.getTcpPort(), h.getHttpPort(),
                                    h.getWsPort(), h.getUdpPort());
                            serverList.add(h);
                            rebalanceNeeded = true;
                        }
                        healthChecker.setTimelyHosts(serverList);

                        if (isLeader.get()) {
                            for (String s : reassignMetrics) {
                                TimelyBalancedHost h = getRoundRobinHost(null);
                                assignMetric(s, h);
                                LOG.debug("Assigned server removed.  Assigning {} to server {}:{}", s, h.getTcpPort());
                            }
                            writeAssigmentsToHdfs();
                        }

                    } finally {
                        balancerLock.writeLock().unlock();
                    }
                    if (isLeader.get() && rebalanceNeeded) {
                        balance();
                    }
                }

                @Override
                public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
                    LOG.info("serviceCache state changed.  Connected:{}", connectionState.isConnected());
                }
            };
            serviceCache.addListener(listener);
            serviceCache.start();

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private TimelyBalancedHost getLeastUsedHost() {

        TimelyBalancedHost tbh = null;
        balancerLock.readLock().lock();
        try {
            Map<Double, TimelyBalancedHost> rateSortedHosts = new TreeMap<>();
            for (TimelyBalancedHost s : serverList) {
                rateSortedHosts.put(s.getArrivalRate(), s);
            }

            Iterator<Map.Entry<Double, TimelyBalancedHost>> itr = rateSortedHosts.entrySet().iterator();

            while (itr.hasNext() && tbh == null) {
                TimelyBalancedHost currentTBH = itr.next().getValue();
                if (currentTBH.isUp()) {
                    tbh = currentTBH;
                }
            }
        } finally {
            balancerLock.readLock().unlock();
        }
        return tbh;
    }

    private TimelyBalancedHost getRandomHost(TimelyBalancedHost notThisOne) {

        TimelyBalancedHost tbh = null;
        balancerLock.readLock().lock();
        try {
            for (int x = 0; tbh == null && x < serverList.size(); x++) {
                tbh = serverList.get(Math.abs(r.nextInt() & Integer.MAX_VALUE) % serverList.size());
                if (!tbh.isUp()) {
                    tbh = null;
                } else if (notThisOne != null && tbh.equals(notThisOne)) {
                    tbh = null;
                }
            }
        } finally {
            balancerLock.readLock().unlock();
        }
        return tbh;
    }

    private TimelyBalancedHost getRoundRobinHost(TimelyBalancedHost notThisOne) {
        TimelyBalancedHost tbh = null;
        balancerLock.readLock().lock();
        try {
            int maxAttempts = serverList.size();
            int currentAttempt = 0;
            while (tbh == null && currentAttempt < maxAttempts) {
                try {
                    currentAttempt++;
                    tbh = serverList.get(roundRobinCounter % serverList.size());
                    if (!tbh.isUp()) {
                        tbh = null;
                    }
                    if (notThisOne != null && notThisOne.equals(tbh)) {
                        tbh = null;
                    }
                } finally {
                    roundRobinCounter++;
                    if (roundRobinCounter == Integer.MAX_VALUE) {
                        roundRobinCounter = 0;
                    }
                }
            }
            if (tbh == null) {
                tbh = getRandomHost(notThisOne);
            }
        } finally {
            balancerLock.readLock().unlock();
        }
        return tbh;
    }

    private TimelyBalancedHost chooseHost(Set<TimelyBalancedHost> potentialHosts,
            Map<TimelyBalancedHost, Double> calculatedRates, double referenceRate, BalanceType balanceType) {

        TimelyBalancedHost tbh = null;
        Map<Long, TimelyBalancedHost> weightedList = new TreeMap<>();
        long cumulativeWeight = 0;
        for (TimelyBalancedHost h : potentialHosts) {
            double currentDiff;
            if (balanceType.equals(BalanceType.HIGH_LOW) || balanceType.equals(BalanceType.HIGH_AVG)) {
                currentDiff = referenceRate - calculatedRates.get(h);
            } else {
                currentDiff = calculatedRates.get(h) - referenceRate;
            }
            cumulativeWeight += Math.round(currentDiff);
            weightedList.put(cumulativeWeight, h);
        }

        if (cumulativeWeight > 0) {
            long randomWeight = r.nextLong() % cumulativeWeight;
            for (Map.Entry<Long, TimelyBalancedHost> e : weightedList.entrySet()) {
                if (randomWeight <= e.getKey()) {
                    tbh = e.getValue();
                    break;
                }
            }
        }
        return tbh;
    }

    private int balance() {

        int numReassigned = 0;
        if (isLeader.get()) {
            double controlBandPercentage = balancerConfig.getControlBandPercentage();
            // save current rates so that we can modify
            double totalArrivalRate = 0;
            Map<TimelyBalancedHost, Double> calculatedRates = new HashMap<>();
            for (TimelyBalancedHost h : serverList) {
                double tbhArrivalRate = h.getArrivalRate();
                calculatedRates.put(h, tbhArrivalRate);
                totalArrivalRate += tbhArrivalRate;
            }
            double averageArrivalRate = totalArrivalRate / serverList.size();

            Set<TimelyBalancedHost> highHosts = new HashSet<>();
            Set<TimelyBalancedHost> lowHosts = new HashSet<>();
            Set<TimelyBalancedHost> avgHosts = new HashSet<>();

            double controlHighLimit = averageArrivalRate * (1.0 + controlBandPercentage);
            double controlLowLimit = averageArrivalRate * (1.0 - controlBandPercentage);

            for (TimelyBalancedHost h : serverList) {
                if (h.isUp()) {
                    double currRate = calculatedRates.get(h);
                    if (currRate < controlLowLimit) {
                        lowHosts.add(h);
                    } else if (currRate > controlHighLimit) {
                        highHosts.add(h);
                    } else {
                        avgHosts.add(h);
                    }
                }
            }

            if (highHosts.isEmpty() && lowHosts.isEmpty()) {
                LOG.info("Host's arrival rates are within {} of the average", controlBandPercentage);
            } else if (!highHosts.isEmpty() && !lowHosts.isEmpty()) {
                LOG.info("begin rebalancing {}", BalanceType.HIGH_LOW);
                numReassigned = rebalance(highHosts, lowHosts, calculatedRates, averageArrivalRate,
                        BalanceType.HIGH_LOW);
                LOG.info("end rebalancing {} - reassigned {}", BalanceType.HIGH_LOW, numReassigned);
            } else if (lowHosts.isEmpty()) {
                LOG.info("begin rebalancing {}", BalanceType.HIGH_AVG);
                numReassigned = rebalance(highHosts, avgHosts, calculatedRates, averageArrivalRate,
                        BalanceType.HIGH_AVG);
                LOG.info("end rebalancing {} - reassigned {}", BalanceType.HIGH_AVG, numReassigned);
            } else {
                LOG.info("begin rebalancing {}", BalanceType.AVG_LOW);
                numReassigned = rebalance(avgHosts, lowHosts, calculatedRates, averageArrivalRate, BalanceType.AVG_LOW);
                LOG.info("end rebalancing {} - reassigned {}", BalanceType.AVG_LOW, numReassigned);
            }
        }
        return numReassigned;
    }

    public int rebalance(Set<TimelyBalancedHost> losingHosts, Set<TimelyBalancedHost> gainingHosts,
            Map<TimelyBalancedHost, Double> calculatedRates, double targetArrivalRate, BalanceType balanceType) {

        int numReassigned = 0;
        if (isLeader.get()) {
            balancerLock.writeLock().lock();
            try {
                Map<String, ArrivalRate> tempMetricMap = new HashMap<>();
                tempMetricMap.putAll(metricMap);

                Set<TimelyBalancedHost> focusedHosts;
                Set<TimelyBalancedHost> selectFromHosts;
                if (balanceType.equals(BalanceType.HIGH_LOW) || balanceType.equals(BalanceType.HIGH_AVG)) {
                    focusedHosts = losingHosts;
                    selectFromHosts = gainingHosts;
                } else {
                    focusedHosts = gainingHosts;
                    selectFromHosts = losingHosts;
                }

                for (TimelyBalancedHost h : focusedHosts) {
                    double desiredChange;
                    if (balanceType.equals(BalanceType.HIGH_LOW) || balanceType.equals(BalanceType.HIGH_AVG)) {
                        desiredChange = h.getArrivalRate() - targetArrivalRate;
                    } else {
                        desiredChange = targetArrivalRate - h.getArrivalRate();
                    }

                    // sort metrics by rate
                    Map<Double, String> rateSortedMetrics = new TreeMap<>(Collections.reverseOrder());
                    for (Map.Entry<String, ArrivalRate> e : tempMetricMap.entrySet()) {
                        if (metricToHostMap.get(e.getKey()).equals(h)) {
                            rateSortedMetrics.put(e.getValue().getRate(), e.getKey());
                        }
                    }

                    LOG.trace("focusHost {}:{} desiredChange:{} rateSortedMetrics.size():{}", h.getHost(),
                            h.getTcpPort(), desiredChange, rateSortedMetrics.size());

                    boolean doneWithHost = false;
                    while (!doneWithHost) {
                        Iterator<Map.Entry<Double, String>> itr = rateSortedMetrics.entrySet().iterator();
                        while (!doneWithHost && itr.hasNext()) {
                            Map.Entry<Double, String> e = null;
                            // find largest metric that does not exceed desired change
                            while (e == null && itr.hasNext()) {
                                e = itr.next();
                                if (e.getKey() > desiredChange) {
                                    LOG.trace("Skipping:{} rate:{}", e.getValue(), e.getKey());
                                    e = null;
                                }
                            }

                            if (e == null) {
                                LOG.trace("no metric small enough");
                                doneWithHost = true;
                            } else {
                                LOG.trace("Selected:{} rate:{}", e.getValue(), e.getKey());
                                TimelyBalancedHost candidateHost = chooseHost(selectFromHosts, calculatedRates,
                                        calculatedRates.get(h), balanceType);
                                if (candidateHost == null) {
                                    LOG.trace("candidate host is null");
                                    doneWithHost = true;
                                } else {
                                    String metric = e.getValue();
                                    Double metricRate = e.getKey();
                                    assignMetric(metric, candidateHost);
                                    numReassigned++;
                                    calculatedRates.put(candidateHost, calculatedRates.get(candidateHost) + metricRate);
                                    calculatedRates.put(h, calculatedRates.get(h) - metricRate);
                                    desiredChange -= metricRate;
                                    // don't move this metric again this host or balance
                                    itr.remove();
                                    tempMetricMap.remove(metric);
                                    LOG.info(
                                            "rebalancing: reassigning metric:{} rate:{} from server {}:{} to {}:{} remaining delta {}",
                                            metric, metricRate, h.getHost(), h.getTcpPort(), candidateHost.getHost(),
                                            candidateHost.getTcpPort(), desiredChange);
                                }
                            }
                        }
                        if (!itr.hasNext()) {
                            LOG.trace("Reached end or rateSortedMetrics");
                            doneWithHost = true;
                        }
                        if (balanceType.equals(BalanceType.HIGH_LOW) || balanceType.equals(BalanceType.HIGH_AVG)) {
                            if (calculatedRates.get(h) <= targetArrivalRate) {
                                doneWithHost = true;
                                LOG.trace("calculatedRates.get(h) <= targetArivalRate");
                            }
                        } else {
                            if (calculatedRates.get(h) >= targetArrivalRate) {
                                doneWithHost = true;
                                LOG.trace("calculatedRates.get(h) >= targetArivalRate");
                            }
                        }
                    }
                }
            } finally {
                balancerLock.writeLock().unlock();
            }
        }
        return numReassigned;
    }

    private boolean shouldCache(String metricName) {

        if (StringUtils.isBlank(metricName)) {
            return false;
        } else {
            balancerLock.readLock().lock();
            try {
                if (metricToHostMap.containsKey(metricName)) {
                    return true;
                }
            } finally {
                balancerLock.readLock().unlock();
            }

            nonCachedMetricsLock.readLock().lock();
            try {
                if (nonCachedMetrics.contains(metricName)) {
                    return false;
                }

                for (String r : nonCachedMetrics) {
                    if (metricName.matches(r)) {
                        LOG.info("Adding {} to list of non-cached metrics", metricName);
                        nonCachedMetricsLock.readLock().unlock();
                        nonCachedMetricsLock.writeLock().lock();
                        nonCachedMetrics.add(metricName);
                        return false;
                    }
                }
            } finally {
                if (nonCachedMetricsLock.isWriteLockedByCurrentThread()) {
                    nonCachedMetricsLock.writeLock().unlock();
                } else {
                    nonCachedMetricsLock.readLock().unlock();
                }
            }
            return true;
        }
    }

    @Override
    public TimelyBalancedHost getHostPortKeyIngest(String metric) {

        TimelyBalancedHost tbh = null;
        balancerLock.readLock().lock();
        try {
            boolean chooseMetricSpecificHost = shouldCache(metric) ? true : false;
            if (chooseMetricSpecificHost) {
                ArrivalRate rate = metricMap.get(metric);
                if (rate == null) {
                    rate = new ArrivalRate(arrivalRateTimer);
                    if (!balancerLock.isWriteLockedByCurrentThread()) {
                        balancerLock.readLock().unlock();
                        balancerLock.writeLock().lock();
                    }
                    metricMap.put(metric, rate);
                }
                rate.arrived();
            } else {
                metric = null;
            }

            if (StringUtils.isBlank(metric)) {
                tbh = getRandomHost(null);
            } else {
                tbh = metricToHostMap.get(metric);
                if (tbh == null) {
                    tbh = getRoundRobinHost(null);
                    if (!balancerLock.isWriteLockedByCurrentThread()) {
                        balancerLock.readLock().unlock();
                        balancerLock.writeLock().lock();
                    }
                    assignMetric(metric, tbh);
                } else if (!tbh.isUp()) {
                    TimelyBalancedHost oldTbh = tbh;
                    tbh = getLeastUsedHost();
                    LOG.info("rebalancing from host that is down: reassigning metric {} from server {}:{} to {}:{}",
                            metric, oldTbh.getHost(), oldTbh.getTcpPort(), tbh.getHost(), tbh.getTcpPort());
                    if (!balancerLock.isWriteLockedByCurrentThread()) {
                        balancerLock.readLock().unlock();
                        balancerLock.writeLock().lock();
                    }
                    assignMetric(metric, tbh);
                }
            }

            // if all else fails
            if (tbh == null || !tbh.isUp()) {
                for (TimelyBalancedHost h : serverList) {
                    if (h.isUp()) {
                        tbh = h;
                        break;
                    }
                }
                if (tbh != null && StringUtils.isNotBlank(metric)) {
                    if (!balancerLock.isWriteLockedByCurrentThread()) {
                        balancerLock.readLock().unlock();
                        balancerLock.writeLock().lock();
                    }
                    assignMetric(metric, tbh);
                }
            }
        } finally {
            if (balancerLock.isWriteLockedByCurrentThread()) {
                balancerLock.writeLock().unlock();
            } else {
                balancerLock.readLock().unlock();
            }
        }
        if (tbh != null) {
            tbh.arrived();
        }
        return tbh;
    }

    @Override
    public TimelyBalancedHost getHostPortKey(String metric) {
        TimelyBalancedHost tbh = null;

        balancerLock.readLock().lock();
        try {
            boolean chooseMetricSpecificHost = shouldCache(metric) ? true : false;
            if (chooseMetricSpecificHost) {
                tbh = metricToHostMap.get(metric);
            } else {
                metric = null;
            }

            if (tbh == null || !tbh.isUp()) {
                for (int x = 0; tbh == null && x < serverList.size(); x++) {
                    tbh = serverList.get(Math.abs(r.nextInt() & Integer.MAX_VALUE) % serverList.size());
                    if (!tbh.isUp()) {
                        tbh = null;
                    }
                }
                if (tbh != null && StringUtils.isNotBlank(metric)) {
                    if (!balancerLock.isWriteLockedByCurrentThread()) {
                        balancerLock.readLock().unlock();
                        balancerLock.writeLock().lock();
                    }
                    assignMetric(metric, tbh);
                }
            }

            // if all else fails
            if (tbh == null || !tbh.isUp()) {
                for (TimelyBalancedHost h : serverList) {
                    if (h.isUp()) {
                        tbh = h;
                        break;
                    }
                }
                if (tbh != null && StringUtils.isNotBlank(metric)) {
                    if (!balancerLock.isWriteLockedByCurrentThread()) {
                        balancerLock.readLock().unlock();
                        balancerLock.writeLock().lock();
                    }
                    assignMetric(metric, tbh);
                }
            }
        } finally {
            if (balancerLock.isWriteLockedByCurrentThread()) {
                balancerLock.writeLock().unlock();
            } else {
                balancerLock.readLock().unlock();
            }
        }
        return tbh;
    }

    private TimelyBalancedHost findHost(String host, int tcpPort) {
        TimelyBalancedHost tbh = null;
        for (TimelyBalancedHost h : serverList) {
            if (h.getHost().equals(host) && h.getTcpPort() == tcpPort) {
                tbh = h;
                break;
            }
        }
        return tbh;
    }

    private void readAssignmentsFromHdfs() {

        Map<String, TimelyBalancedHost> assignedMetricToHostMap = new TreeMap<>();
        try {
            assignmentsLock.readLock().acquire();
            Configuration configuration = new Configuration();
            FileSystem fs = FileSystem.get(configuration);
            Path assignmentFile = new Path(balancerConfig.getAssignmentFile());
            FSDataInputStream iStream = fs.open(assignmentFile);

            CsvReader reader = new CsvReader(iStream, ',', Charset.forName("UTF-8"));
            reader.setUseTextQualifier(false);

            // skip the headers
            boolean success = true;
            success = reader.readHeaders();

            while (success) {
                success = reader.readRecord();
                String[] nextLine = reader.getValues();
                if (nextLine.length >= 3) {
                    String metric = nextLine[0];
                    String host = nextLine[1];
                    int tcpPort = Integer.parseInt(nextLine[2]);
                    TimelyBalancedHost tbh = findHost(host, tcpPort);
                    if (tbh == null) {
                        tbh = getRoundRobinHost(null);
                    } else {
                        LOG.trace("Found assigment: {} to {}:{}", metric, host, tcpPort);
                    }
                    assignedMetricToHostMap.put(metric, tbh);
                }
            }

            balancerLock.writeLock().lock();
            try {
                metricToHostMap.clear();
                for (Map.Entry<String, TimelyBalancedHost> e : assignedMetricToHostMap.entrySet()) {
                    if (shouldCache(e.getKey())) {
                        metricToHostMap.put(e.getKey(), e.getValue());
                    }
                }
            } finally {
                balancerLock.writeLock().unlock();
            }
            assignmentsLastUpdatedLocal.set(assignmentsLastUpdatedInHdfs.get().postValue());
            LOG.info("Read {} assignments from hdfs lastHdfsUpdate = lastLocalUpdate ({})", metricToHostMap.size(),
                    new Date(assignmentsLastUpdatedLocal.get()));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                assignmentsLock.readLock().release();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private void writeAssigmentsToHdfs() {

        CsvWriter writer = null;
        try {
            assignmentsLock.writeLock().acquire();
            balancerLock.readLock().lock();
            try {
                if (!metricToHostMap.isEmpty()) {
                    Configuration configuration = new Configuration();
                    FileSystem fs = FileSystem.get(configuration);
                    Path assignmentFile = new Path(balancerConfig.getAssignmentFile());
                    if (!fs.exists(assignmentFile.getParent())) {
                        fs.mkdirs(assignmentFile.getParent());
                    }
                    FSDataOutputStream oStream = fs.create(assignmentFile, true);
                    writer = new CsvWriter(oStream, ',', Charset.forName("UTF-8"));
                    writer.setUseTextQualifier(false);
                    writer.write("metric");
                    writer.write("host");
                    writer.write("tcpPort");
                    writer.endRecord();
                    for (Map.Entry<String, TimelyBalancedHost> e : metricToHostMap.entrySet()) {
                        writer.write(e.getKey());
                        writer.write(e.getValue().getHost());
                        writer.write(Integer.toString(e.getValue().getTcpPort()));
                        writer.endRecord();
                        LOG.trace("Saving assigment: {} to {}:{}", e.getKey(), e.getValue().getHost(),
                                e.getValue().getTcpPort());
                    }

                    long now = System.currentTimeMillis();
                    assignmentsLastUpdatedLocal.set(now);
                    assignmentsLastUpdatedInHdfs.trySet(now);
                    if (!assignmentsLastUpdatedInHdfs.get().succeeded()) {
                        assignmentsLastUpdatedInHdfs.forceSet(now);
                    }
                    LOG.info("Wrote {} assignments to hdfs lastHdfsUpdate = lastLocalUpdate ({})",
                            metricToHostMap.size(), new Date(assignmentsLastUpdatedLocal.get()));
                }
            } finally {
                balancerLock.readLock().unlock();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (writer != null) {
                writer.close();
            }
            try {
                assignmentsLock.writeLock().release();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private void assignMetric(String metric, TimelyBalancedHost tbh) {
        if (isLeader.get()) {
            balancerLock.writeLock().lock();
            try {
                metricToHostMap.put(metric, tbh);
                assignmentsLastUpdatedLocal.set(System.currentTimeMillis());
            } finally {
                balancerLock.writeLock().unlock();
            }
        }
    }
}
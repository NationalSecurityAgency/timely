package timely.balancer.resolver;

import static timely.Server.SERVICE_DISCOVERY_PATH;
import static timely.balancer.Balancer.ASSIGNMENTS_LAST_UPDATED_PATH;
import static timely.balancer.Balancer.ASSIGNMENTS_LOCK_PATH;
import static timely.balancer.Balancer.LEADER_LATCH_PATH;
import static timely.store.cache.DataStoreCache.NON_CACHED_METRICS;
import static timely.store.cache.DataStoreCache.NON_CACHED_METRICS_LOCK_PATH;

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
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicValue;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.RetryForever;
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
import org.apache.zookeeper.CreateMode;
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
    private Timer arrivalRateTimer = new Timer("ArrivalRateTimerResolver", true);
    private int roundRobinCounter = 0;
    private Set<String> nonCachedMetrics = new HashSet<>();
    private DistributedAtomicValue nonCachedMetricsIP;
    private ReentrantReadWriteLock nonCachedMetricsLocalLock = new ReentrantReadWriteLock();
    private InterProcessReadWriteLock nonCachedMetricsIPRWLock;
    private BalancerConfiguration balancerConfig;
    private LeaderLatch leaderLatch;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private InterProcessReadWriteLock assignmentsIPRWLock;
    private DistributedAtomicLong assignmentsLastUpdatedInHdfs;
    private AtomicLong assignmentsLastUpdatedLocal = new AtomicLong(0);

    private enum BalanceType {
        HIGH_LOW, HIGH_AVG, AVG_LOW
    }

    public BalancedMetricResolver(CuratorFramework curatorFramework, BalancerConfiguration balancerConfig,
            HealthChecker healthChecker) {
        this.balancerConfig = balancerConfig;
        this.healthChecker = healthChecker;

        assignmentsIPRWLock = new InterProcessReadWriteLock(curatorFramework, ASSIGNMENTS_LOCK_PATH);
        testIPRWLock(curatorFramework, assignmentsIPRWLock, ASSIGNMENTS_LOCK_PATH);
        startLeaderLatch(curatorFramework);
        startServiceListener(curatorFramework);
        assignmentsLastUpdatedInHdfs = new DistributedAtomicLong(curatorFramework, ASSIGNMENTS_LAST_UPDATED_PATH,
                new RetryForever(1000));

        TreeCacheListener assignmentListener = new TreeCacheListener() {

            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                LOG.debug("Handling assignments event {}. assignmentsLastUpdatedInHdfs:{}", event.getType().toString(),
                        new Date(assignmentsLastUpdatedInHdfs.get().postValue()));
                if (event.getType().equals(TreeCacheEvent.Type.NODE_UPDATED)) {
                    long lastLocalUpdate = assignmentsLastUpdatedLocal.get();
                    long lastHdfsUpdate = assignmentsLastUpdatedInHdfs.get().postValue();
                    if (lastHdfsUpdate > lastLocalUpdate) {
                        LOG.debug("Reading assignments from hdfs lastHdfsUpdate ({}) > lastLocalUpdate ({})",
                                new Date(lastHdfsUpdate), new Date(lastLocalUpdate));
                        readAssignmentsFromHdfs();
                    } else {
                        LOG.debug("Not reading assignments from hdfs lastHdfsUpdate ({}) <= lastLocalUpdate ({})",
                                new Date(lastHdfsUpdate), new Date(lastLocalUpdate));
                    }
                }
            }
        };

        try {
            TreeCache assignmentTreeCache = new TreeCache(curatorFramework, ASSIGNMENTS_LAST_UPDATED_PATH);
            assignmentTreeCache.getListenable().addListener(assignmentListener);
            assignmentTreeCache.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        nonCachedMetricsIPRWLock = new InterProcessReadWriteLock(curatorFramework, NON_CACHED_METRICS_LOCK_PATH);
        testIPRWLock(curatorFramework, nonCachedMetricsIPRWLock, NON_CACHED_METRICS_LOCK_PATH);
        nonCachedMetricsIP = new DistributedAtomicValue(curatorFramework, NON_CACHED_METRICS, new RetryForever(1000));
        TreeCacheListener nonCachedMetricsListener = new TreeCacheListener() {

            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent event) throws Exception {
                if (event.getType().equals(TreeCacheEvent.Type.NODE_UPDATED)) {
                    LOG.debug("Handling nonCachedMetricsIP event {}", event.getType().toString());
                    readNonCachedMetricsIP();
                }
            }
        };

        try {
            TreeCache nonCachedMetricsTreeCache = new TreeCache(curatorFramework, NON_CACHED_METRICS);
            nonCachedMetricsTreeCache.getListenable().addListener(nonCachedMetricsListener);
            nonCachedMetricsTreeCache.start();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        nonCachedMetricsLocalLock.writeLock().lock();
        try {
            addNonCachedMetrics(balancerConfig.getCache().getNonCachedMetrics());
        } finally {
            nonCachedMetricsLocalLock.writeLock().unlock();
        }
        readNonCachedMetricsIP();

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
                            LOG.debug("Writing assignments to hdfs lastLocalUpdate ({}) > lastHdfsUpdate ({})",
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

    private void readNonCachedMetricsIP() {

        Set<String> currentNonCachedMetricsDistributed = new TreeSet<>();
        try {
            boolean acquired = false;
            while (!acquired) {
                acquired = nonCachedMetricsIPRWLock.readLock().acquire(60, TimeUnit.SECONDS);
            }
            byte[] currentNonCachedMetricsDistributedBytes = nonCachedMetricsIP.get().postValue();
            if (currentNonCachedMetricsDistributedBytes != null) {
                try {
                    currentNonCachedMetricsDistributed = SerializationUtils
                            .deserialize(currentNonCachedMetricsDistributedBytes);
                } catch (Exception e) {
                    LOG.error(e.getMessage());
                    currentNonCachedMetricsDistributed = new TreeSet<>();
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                nonCachedMetricsIPRWLock.readLock().release();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        nonCachedMetricsLocalLock.writeLock().lock();
        try {
            if (nonCachedMetrics.containsAll(currentNonCachedMetricsDistributed)) {
                LOG.debug("local nonCachedMetrics already contains {}", currentNonCachedMetricsDistributed);
            } else {
                currentNonCachedMetricsDistributed.removeAll(nonCachedMetrics);
                LOG.debug("Adding {} to local nonCachedMetrics", currentNonCachedMetricsDistributed);
                nonCachedMetrics.addAll(currentNonCachedMetricsDistributed);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            nonCachedMetricsLocalLock.writeLock().unlock();
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
            ServiceDiscovery<ServerDetails> discovery = ServiceDiscoveryBuilder.builder(ServerDetails.class)
                    .client(curatorFramework).basePath(SERVICE_DISCOVERY_PATH).build();
            discovery.start();
            Collection<ServiceInstance<ServerDetails>> instances = discovery.queryForInstances("timely-server");

            balancerLock.writeLock().lock();
            try {
                for (ServiceInstance<ServerDetails> si : instances) {
                    ServerDetails pl = si.getPayload();
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

            final ServiceCache<ServerDetails> serviceCache = discovery.serviceCacheBuilder().name("timely-server")
                    .build();
            ServiceCacheListener listener = new ServiceCacheListener() {

                @Override
                public void cacheChanged() {
                    boolean rebalanceNeeded = false;
                    balancerLock.writeLock().lock();
                    try {
                        List<ServiceInstance<ServerDetails>> instances = serviceCache.getInstances();
                        Set<TimelyBalancedHost> availableHosts = new HashSet<>();
                        for (ServiceInstance<ServerDetails> si : instances) {
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
                LOG.debug("Host's arrival rates are within {} of the average", controlBandPercentage);
            } else if (!highHosts.isEmpty() && !lowHosts.isEmpty()) {
                LOG.debug("begin rebalancing {}", BalanceType.HIGH_LOW);
                numReassigned = rebalance(highHosts, lowHosts, calculatedRates, averageArrivalRate,
                        BalanceType.HIGH_LOW);
                LOG.debug("end rebalancing {} - reassigned {}", BalanceType.HIGH_LOW, numReassigned);
            } else if (lowHosts.isEmpty()) {
                LOG.debug("begin rebalancing {}", BalanceType.HIGH_AVG);
                numReassigned = rebalance(highHosts, avgHosts, calculatedRates, averageArrivalRate,
                        BalanceType.HIGH_AVG);
                LOG.debug("end rebalancing {} - reassigned {}", BalanceType.HIGH_AVG, numReassigned);
            } else {
                LOG.debug("begin rebalancing {}", BalanceType.AVG_LOW);
                numReassigned = rebalance(avgHosts, lowHosts, calculatedRates, averageArrivalRate, BalanceType.AVG_LOW);
                LOG.debug("end rebalancing {} - reassigned {}", BalanceType.AVG_LOW, numReassigned);
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

    private void addNonCachedMetrics(Collection<String> nonCachedMetricsUpdate) {
        if (!nonCachedMetricsUpdate.isEmpty()) {
            try {
                LOG.debug("Adding {} to local nonCachedMetrics", nonCachedMetricsUpdate);
                nonCachedMetricsLocalLock.writeLock().lock();
                try {
                    nonCachedMetrics.addAll(nonCachedMetricsUpdate);
                } finally {
                    nonCachedMetricsLocalLock.writeLock().unlock();
                }

                try {
                    boolean acquired = false;
                    while (!acquired) {
                        acquired = nonCachedMetricsIPRWLock.writeLock().acquire(60, TimeUnit.SECONDS);
                    }
                    byte[] currentNonCachedMetricsDistributedBytes = nonCachedMetricsIP.get().postValue();
                    Set<String> currentNonCachedMetricsIP;
                    if (currentNonCachedMetricsDistributedBytes == null) {
                        currentNonCachedMetricsIP = new TreeSet<>();
                    } else {
                        currentNonCachedMetricsIP = SerializationUtils
                                .deserialize(currentNonCachedMetricsDistributedBytes);
                    }
                    if (currentNonCachedMetricsIP.containsAll(nonCachedMetricsUpdate)) {
                        LOG.debug("nonCachedMetricsIP already contains {}", nonCachedMetricsUpdate);
                    } else {
                        nonCachedMetricsUpdate.removeAll(currentNonCachedMetricsIP);
                        LOG.debug("Adding {} to nonCachedMetricsIP", nonCachedMetricsUpdate);
                        TreeSet<String> updateSet = new TreeSet<>();
                        updateSet.addAll(currentNonCachedMetricsIP);
                        updateSet.addAll(nonCachedMetricsUpdate);
                        byte[] updateValue = SerializationUtils.serialize(updateSet);
                        nonCachedMetricsIP.trySet(updateValue);
                        if (!nonCachedMetricsIP.get().succeeded()) {
                            nonCachedMetricsIP.forceSet(updateValue);
                        }
                    }
                } finally {
                    nonCachedMetricsIPRWLock.writeLock().release();
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private void testIPRWLock(CuratorFramework curatorFramework, InterProcessReadWriteLock lock, String path) {
        try {
            lock.writeLock().acquire(10, TimeUnit.SECONDS);
        } catch (Exception e1) {
            try {
                curatorFramework.delete().deletingChildrenIfNeeded().forPath(path);
                curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                        .forPath(path);
            } catch (Exception e2) {
                LOG.info(e2.getMessage(), e2);
            }
        } finally {
            try {
                lock.writeLock().release();
            } catch (Exception e3) {
                LOG.error(e3.getMessage());
            }
        }
    }

    private boolean shouldCache(String metricName) {

        if (StringUtils.isBlank(metricName)) {
            return false;
        } else {
            balancerLock.readLock().lock();
            try {
                nonCachedMetricsLocalLock.readLock().lock();
                try {
                    if (nonCachedMetrics.contains(metricName)) {
                        return false;
                    }
                } finally {
                    nonCachedMetricsLocalLock.readLock().unlock();
                }
                if (metricToHostMap.containsKey(metricName)) {
                    return true;
                }
            } finally {
                balancerLock.readLock().unlock();
            }

            nonCachedMetricsLocalLock.writeLock().lock();
            try {
                for (String r : nonCachedMetrics) {
                    if (metricName.matches(r)) {
                        LOG.debug("Adding {} to list of non-cached metrics", metricName);
                        addNonCachedMetrics(Collections.singleton(metricName));
                        return false;
                    }
                }
            } finally {
                nonCachedMetricsLocalLock.writeLock().unlock();
            }
            return true;
        }
    }

    @Override
    public TimelyBalancedHost getHostPortKeyIngest(String metric) {

        TimelyBalancedHost tbh = null;
        balancerLock.readLock().lock();
        boolean chooseMetricSpecificHost;
        try {
            chooseMetricSpecificHost = shouldCache(metric) ? true : false;
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
                    LOG.debug("rebalancing from host that is down: reassigning metric {} from server {}:{} to {}:{}",
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
        if (tbh != null && chooseMetricSpecificHost) {
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
            boolean acquired = false;
            while (!acquired) {
                acquired = assignmentsIPRWLock.readLock().acquire(60, TimeUnit.SECONDS);
            }
            balancerLock.writeLock().lock();
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

            metricToHostMap.clear();
            for (Map.Entry<String, TimelyBalancedHost> e : assignedMetricToHostMap.entrySet()) {
                if (shouldCache(e.getKey())) {
                    metricToHostMap.put(e.getKey(), e.getValue());
                }
            }

            nonCachedMetricsLocalLock.readLock().lock();
            try {
                Iterator<Map.Entry<String, TimelyBalancedHost>> itr = metricToHostMap.entrySet().iterator();
                while (itr.hasNext()) {
                    Map.Entry<String, TimelyBalancedHost> e = itr.next();
                    if (nonCachedMetrics.contains(e.getKey())) {
                        itr.remove();
                    }
                }
            } finally {
                nonCachedMetricsLocalLock.readLock().unlock();
            }
            assignmentsLastUpdatedLocal.set(assignmentsLastUpdatedInHdfs.get().postValue());
            LOG.info("Read {} assignments from hdfs lastHdfsUpdate = lastLocalUpdate ({})", metricToHostMap.size(),
                    new Date(assignmentsLastUpdatedLocal.get()));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            balancerLock.writeLock().unlock();
            try {
                assignmentsIPRWLock.readLock().release();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    private void writeAssigmentsToHdfs() {

        CsvWriter writer = null;
        try {
            boolean acquired = false;
            while (!acquired) {
                acquired = assignmentsIPRWLock.writeLock().acquire(60, TimeUnit.SECONDS);
            }
            balancerLock.readLock().lock();
            nonCachedMetricsLocalLock.readLock().lock();
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
                        if (!nonCachedMetrics.contains(e.getKey())) {
                            writer.write(e.getKey());
                            writer.write(e.getValue().getHost());
                            writer.write(Integer.toString(e.getValue().getTcpPort()));
                            writer.endRecord();
                            LOG.trace("Saving assigment: {} to {}:{}", e.getKey(), e.getValue().getHost(),
                                    e.getValue().getTcpPort());
                        }
                    }

                    long now = System.currentTimeMillis();
                    assignmentsLastUpdatedLocal.set(now);
                    assignmentsLastUpdatedInHdfs.trySet(now);
                    if (!assignmentsLastUpdatedInHdfs.get().succeeded()) {
                        assignmentsLastUpdatedInHdfs.forceSet(now);
                    }
                    LOG.debug("Wrote {} assignments to hdfs lastHdfsUpdate = lastLocalUpdate ({})",
                            metricToHostMap.size(), new Date(assignmentsLastUpdatedLocal.get()));
                }
            } finally {
                nonCachedMetricsLocalLock.readLock().unlock();
                balancerLock.readLock().unlock();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            if (writer != null) {
                writer.close();
            }
            try {
                assignmentsIPRWLock.writeLock().release();
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

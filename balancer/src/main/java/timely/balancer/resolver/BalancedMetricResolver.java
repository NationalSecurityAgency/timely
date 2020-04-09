package timely.balancer.resolver;

import static timely.Server.SERVICE_DISCOVERY_PATH;
import static timely.balancer.Balancer.ASSIGNMENTS_LAST_UPDATED_PATH;
import static timely.balancer.Balancer.ASSIGNMENTS_LOCK_PATH;
import static timely.balancer.Balancer.LEADER_LATCH_PATH;
import static timely.balancer.resolver.eventing.MetricAssignedEvent.Reason.ASSIGN_FALLBACK_SEQUENTIAL;
import static timely.balancer.resolver.eventing.MetricAssignedEvent.Reason.ASSIGN_ROUND_ROBIN;
import static timely.balancer.resolver.eventing.MetricAssignedEvent.Reason.HOST_DOWN_ROUND_ROBIN;
import static timely.balancer.resolver.eventing.MetricAssignedEvent.Reason.HOST_REMOVED;
import static timely.balancer.resolver.eventing.MetricAssignedEvent.Reason.REBALANCE;
import static timely.store.cache.DataStoreCache.NON_CACHED_METRICS;
import static timely.store.cache.DataStoreCache.NON_CACHED_METRICS_LOCK_PATH;

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
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.curator.framework.CuratorFramework;
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
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.ServerDetails;
import timely.balancer.ArrivalRate;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.healthcheck.HealthChecker;
import timely.balancer.resolver.eventing.MetricAssignedCallback;
import timely.balancer.resolver.eventing.MetricAssignedEvent;
import timely.balancer.resolver.eventing.MetricBalanceCallback;
import timely.balancer.resolver.eventing.MetricBalanceEvent;
import timely.balancer.resolver.eventing.MetricHostCallback;
import timely.balancer.resolver.eventing.MetricHostEvent;

public class BalancedMetricResolver implements MetricResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BalancedMetricResolver.class);
    private Map<String, TimelyBalancedHost> metricToHostMap = new TreeMap<>();
    private Set<String> unassignedMetrics = new HashSet<>();
    private Map<String, ArrivalRate> metricMap = new HashMap<>();
    private ReentrantReadWriteLock balancerLock = new ReentrantReadWriteLock();
    private List<TimelyBalancedHost> serverList = new ArrayList<>();
    private Random r = new Random();
    private final HealthChecker healthChecker;
    private ScheduledExecutorService assignmentExecutor = Executors.newScheduledThreadPool(2);
    private ScheduledExecutorService arrivalRateExecutor = Executors.newScheduledThreadPool(2);
    private int roundRobinCounter = 0;
    private Set<String> nonCachedMetrics = new HashSet<>();
    private DistributedAtomicValue nonCachedMetricsIP;
    private ReentrantReadWriteLock nonCachedMetricsLocalLock = new ReentrantReadWriteLock();
    private InterProcessReadWriteLock nonCachedMetricsIPRWLock;
    private AtomicLong assignmentsLastUpdatedLocal = new AtomicLong(0);
    private LeaderLatch leaderLatch;
    private AtomicBoolean isLeader = new AtomicBoolean(false);
    private InterProcessReadWriteLock assignmentsIPRWLock;
    private MetricAssignmentPerister assignmentPerister;

    final private BalancerConfiguration balancerConfig;
    final private CuratorFramework curatorFramework;

    private List<MetricAssignedCallback> metricAssignedCallbacks = new ArrayList<>();
    private List<MetricBalanceCallback> metricBalanceCallbacks = new ArrayList<>();
    private List<MetricHostCallback> metricHostCallbacks = new ArrayList<>();

    public enum BalanceType {
        HIGH_LOW, HIGH_AVG, AVG_LOW;
    }

    public BalancedMetricResolver(CuratorFramework curatorFramework, BalancerConfiguration balancerConfig,
            HealthChecker healthChecker) {
        this.curatorFramework = curatorFramework;
        this.balancerConfig = balancerConfig;
        this.healthChecker = healthChecker;
    }

    public void start() {
        assignmentsIPRWLock = new InterProcessReadWriteLock(curatorFramework, ASSIGNMENTS_LOCK_PATH);
        testIPRWLock(curatorFramework, assignmentsIPRWLock, ASSIGNMENTS_LOCK_PATH);

        this.assignmentPerister = new MetricAssignmentPerister(this, balancerConfig, metricToHostMap, nonCachedMetrics,
                curatorFramework, assignmentsIPRWLock, nonCachedMetricsLocalLock, assignmentsLastUpdatedLocal,
                balancerLock);

        startLeaderLatch(curatorFramework);
        startServiceListener(curatorFramework);

        TreeCacheListener assignmentListener = (curatorFramework1, event) -> {
            LOG.debug("Handling assignments event {}. assignmentsLastUpdated:{}", event.getType().toString(),
                    new Date(assignmentPerister.getAssignmentsLastUpdatedByLeader()));
            if (event.getType().equals(TreeCacheEvent.Type.NODE_UPDATED)) {
                assignmentPerister.readAssignments(true);
            }
        };

        try (TreeCache assignmentTreeCache = new TreeCache(curatorFramework, ASSIGNMENTS_LAST_UPDATED_PATH)) {
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

        try (TreeCache nonCachedMetricsTreeCache = new TreeCache(curatorFramework, NON_CACHED_METRICS)) {
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

        assignmentPerister.readAssignments(false);

        assignmentExecutor.scheduleAtFixedRate(() -> {
            if (isLeader.get()) {
                try {
                    balance();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }, balancerConfig.getBalanceDelay(), balancerConfig.getBalancePeriod(), TimeUnit.MILLISECONDS);

        assignmentExecutor.scheduleAtFixedRate(() -> {
            try {
                if (isLeader.get()) {
                    long lastLocalUpdate = assignmentsLastUpdatedLocal.get();
                    long lastUpdateByLeader = assignmentPerister.getAssignmentsLastUpdatedByLeader();
                    if (lastLocalUpdate > lastUpdateByLeader) {
                        LOG.debug("Leader writing assignments lastLocalUpdate ({}) > lastUpdateByLeader ({})",
                                new Date(lastLocalUpdate), new Date(lastUpdateByLeader));
                        assignmentPerister.writeAssignments();
                    } else {
                        LOG.trace("Leader not writing assignments lastLocalUpdate ({}) <= lastUpdateByLeader ({})",
                                new Date(lastLocalUpdate), new Date(lastUpdateByLeader));
                    }
                } else {
                    assignmentPerister.readAssignments(true);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }, balancerConfig.getPersistDelay(), balancerConfig.getPersistPeriod(), TimeUnit.MILLISECONDS);
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
                    LOG.error(e.getMessage(), e);
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
                    assignmentPerister.writeAssignments();
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
                            pl.getWsPort(), pl.getUdpPort(), new ArrivalRate(arrivalRateExecutor));
                    LOG.info("adding service {} host:{}", si.getId(), TimelyBalancedHost.toStringShort(tbh));
                    tbh.setBalancerConfig(balancerConfig);
                    serverList.add(tbh);
                    metricHostEvent(tbh, MetricHostEvent.ActionType.ADDED);
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
                                    pl.getHttpPort(), pl.getWsPort(), pl.getUdpPort(),
                                    new ArrivalRate(arrivalRateExecutor));
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
                                metricHostEvent(h, MetricHostEvent.ActionType.REMOVED);
                                LOG.info("removing service {}", TimelyBalancedHost.toStringShort(h));
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
                            LOG.info("adding service {}", TimelyBalancedHost.toStringShort(h));
                            serverList.add(h);
                            metricHostEvent(h, MetricHostEvent.ActionType.ADDED);
                            rebalanceNeeded = true;
                        }
                        healthChecker.setTimelyHosts(serverList);
                        if (!availableHosts.isEmpty()) {
                            reassignMetrics.addAll(unassignedMetrics);
                            unassignedMetrics.clear();
                        }
                        if (isLeader.get()) {
                            for (String s : reassignMetrics) {
                                TimelyBalancedHost h = getRoundRobinHost(null);
                                assignMetric(s, h, HOST_REMOVED);
                                LOG.debug("Assigned server removed.  Assigning {} to server {}", s,
                                        TimelyBalancedHost.toStringShort(h));
                            }
                        }

                    } finally {
                        balancerLock.writeLock().unlock();
                    }
                    if (isLeader.get()) {
                        if (rebalanceNeeded) {
                            balance();
                        }
                        assignmentPerister.writeAssignments();
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

    protected TimelyBalancedHost getRoundRobinHost(TimelyBalancedHost notThisOne) {
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
            metricBalanceEvent(MetricBalanceEvent.ProgressType.BEGIN, balanceType, 0);
            balancerLock.writeLock().lock();
            try {
                metricBalanceEvent(MetricBalanceEvent.ProgressType.BALANCER_LOCK_ACQUIRED, balanceType, 0);
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

                    LOG.trace("focusHost {} desiredChange:{} rateSortedMetrics.size():{}",
                            TimelyBalancedHost.toStringShort(h), desiredChange, rateSortedMetrics.size());

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
                                    assignMetric(metric, candidateHost, REBALANCE);
                                    numReassigned++;
                                    calculatedRates.put(candidateHost, calculatedRates.get(candidateHost) + metricRate);
                                    calculatedRates.put(h, calculatedRates.get(h) - metricRate);
                                    desiredChange -= metricRate;
                                    // don't move this metric again this host or balance
                                    itr.remove();
                                    tempMetricMap.remove(metric);
                                    LOG.info(
                                            "rebalancing: reassigning metric:{} rate:{} from server {} to {} remaining delta {}",
                                            metric, metricRate, TimelyBalancedHost.toStringShort(h),
                                            TimelyBalancedHost.toStringShort(candidateHost), desiredChange);
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
                metricBalanceEvent(MetricBalanceEvent.ProgressType.END, balanceType, numReassigned);
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
                LOG.error(e3.getMessage(), e3);
            }
        }
    }

    protected boolean shouldCache(String metricName) {

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
                    rate = new ArrivalRate(arrivalRateExecutor);
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
                tbh = getRoundRobinHost(null);
            } else {
                tbh = metricToHostMap.get(metric);
                if (tbh == null) {
                    tbh = getRoundRobinHost(null);
                    if (!balancerLock.isWriteLockedByCurrentThread()) {
                        balancerLock.readLock().unlock();
                        balancerLock.writeLock().lock();
                    }
                    assignMetric(metric, tbh, ASSIGN_ROUND_ROBIN);
                } else if (!tbh.isUp()) {
                    TimelyBalancedHost oldTbh = tbh;
                    tbh = getRoundRobinHost(oldTbh);
                    LOG.debug("rebalancing from host that is down: reassigning metric {} from server {} to {}", metric,
                            TimelyBalancedHost.toStringShort(oldTbh), TimelyBalancedHost.toStringShort(tbh));
                    if (!balancerLock.isWriteLockedByCurrentThread()) {
                        balancerLock.readLock().unlock();
                        balancerLock.writeLock().lock();
                    }
                    assignMetric(metric, tbh, HOST_DOWN_ROUND_ROBIN);
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
                    assignMetric(metric, tbh, ASSIGN_FALLBACK_SEQUENTIAL);
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

            if (tbh == null) {
                tbh = getRoundRobinHost(null);
                if (!balancerLock.isWriteLockedByCurrentThread()) {
                    balancerLock.readLock().unlock();
                    balancerLock.writeLock().lock();
                }
                assignMetric(metric, tbh, ASSIGN_ROUND_ROBIN);
            } else if (!tbh.isUp()) {
                TimelyBalancedHost oldTbh = tbh;
                tbh = getRoundRobinHost(oldTbh);
                LOG.debug("rebalancing from host that is down: reassigning metric {} from server {} to {}", metric,
                        TimelyBalancedHost.toStringShort(oldTbh), TimelyBalancedHost.toStringShort(tbh));
                if (!balancerLock.isWriteLockedByCurrentThread()) {
                    balancerLock.readLock().unlock();
                    balancerLock.writeLock().lock();
                }
                assignMetric(metric, tbh, HOST_DOWN_ROUND_ROBIN);
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
                    assignMetric(metric, tbh, ASSIGN_FALLBACK_SEQUENTIAL);
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

    protected TimelyBalancedHost findHost(String host, int tcpPort) {
        TimelyBalancedHost tbh = null;
        for (TimelyBalancedHost h : serverList) {
            if (h.getHost().equals(host) && h.getTcpPort() == tcpPort) {
                tbh = h;
                break;
            }
        }
        return tbh;
    }

    private void assignMetric(String metric, TimelyBalancedHost tbh, MetricAssignedEvent.Reason reason) {
        if (isLeader.get()) {
            if (StringUtils.isNotBlank(metric) && tbh != null) {
                balancerLock.writeLock().lock();
                try {
                    metricAssignedEvent(metric, metricToHostMap.get(metric), tbh, reason);
                    metricToHostMap.put(metric, tbh);
                    assignmentsLastUpdatedLocal.set(System.currentTimeMillis());
                } finally {
                    balancerLock.writeLock().unlock();
                }
            } else {
                unassignedMetrics.add(metric);
                // Exception e = new IllegalStateException("Bad assignment metric:" + metric + "
                // host:" + tbh);
                LOG.warn("Bad assignment metric:" + metric + " host:" + tbh);
            }
        }
    }

    public void registerCallback(MetricAssignedCallback callback) {
        this.metricAssignedCallbacks.add(callback);
    }

    public void registerCallback(MetricBalanceCallback callback) {
        this.metricBalanceCallbacks.add(callback);
    }

    public void registerCallback(MetricHostCallback callback) {
        this.metricHostCallbacks.add(callback);
    }

    protected void metricAssignedEvent(String metric, TimelyBalancedHost losing, TimelyBalancedHost gaining,
            MetricAssignedEvent.Reason reason) {
        if (!metricAssignedCallbacks.isEmpty()) {
            MetricAssignedEvent event = new MetricAssignedEvent(metric, losing, gaining, reason);
            for (MetricAssignedCallback callback : metricAssignedCallbacks) {
                callback.onEvent(event);
            }
        }
    }

    protected void metricBalanceEvent(MetricBalanceEvent.ProgressType progressType,
            BalancedMetricResolver.BalanceType balanceType, long numReassigned) {
        if (!metricBalanceCallbacks.isEmpty()) {
            MetricBalanceEvent event = new MetricBalanceEvent(progressType, balanceType, numReassigned);
            for (MetricBalanceCallback callback : metricBalanceCallbacks) {
                callback.onEvent(event);
            }
        }
    }

    protected void metricHostEvent(TimelyBalancedHost host, MetricHostEvent.ActionType actionType) {
        if (!metricHostCallbacks.isEmpty()) {
            MetricHostEvent event = new MetricHostEvent(host, actionType);
            for (MetricHostCallback callback : metricHostCallbacks) {
                callback.onEvent(event);
            }
        }
    }

    protected void writeAssignments(Map<String, TimelyBalancedHost> metricToHostMap) {
        assignmentPerister.writeAssignments(metricToHostMap);
    }

    @Override
    public void stop() {

        this.assignmentExecutor.shutdown();
        try {
            this.assignmentExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        } finally {
            if (!this.assignmentExecutor.isTerminated()) {
                this.assignmentExecutor.shutdownNow();
            }
        }

        this.arrivalRateExecutor.shutdown();
        try {
            this.arrivalRateExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {

        } finally {
            if (!this.arrivalRateExecutor.isTerminated()) {
                this.arrivalRateExecutor.shutdownNow();
            }
        }

        try {
            this.healthChecker.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}

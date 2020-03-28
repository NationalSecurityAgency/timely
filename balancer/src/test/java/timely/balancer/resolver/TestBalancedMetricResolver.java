package timely.balancer.resolver;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.SocketUtils;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.resolver.eventing.MetricAssignedCallback;
import timely.balancer.resolver.eventing.MetricAssignedEvent;
import timely.balancer.resolver.eventing.MetricBalanceCallback;
import timely.balancer.resolver.eventing.MetricBalanceEvent;
import timely.balancer.resolver.eventing.MetricHostCallback;
import timely.balancer.resolver.eventing.MetricHostEvent;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class TestBalancedMetricResolver {

    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private CuratorFramework curatorFramework;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
    private Random r = new Random(System.currentTimeMillis());
    private List<TimelyBalancedHost> timelyHosts;
    private Map<MetricAssignedEvent, Integer> metricBalanceEventCounter = new HashMap<>();

    @Before
    public void startup() {
        QuorumPeerConfig currentConfig = createZookeeper();
        InetSocketAddress address = currentConfig.getClientPortAddress();
        String zkServer = address.getHostName() + ":" + address.getPort();
        curatorFramework = CuratorFrameworkFactory.newClient(zkServer, 1000, 5000,
                new RetryNTimes(2, 50));
        curatorFramework.start();
        timelyHosts = new ArrayList<>();
    }

    @After
    public void shutdown() {
        curatorFramework.close();
    }

    private QuorumPeerConfig createZookeeper() {
        Properties startupProperties = new Properties();
        File file = new File(System.getProperty("java.io.tmpdir")
                + File.separator + UUID.randomUUID());
        file.deleteOnExit();
        startupProperties.setProperty("dataDir", file.getAbsolutePath());
        startupProperties.setProperty("clientPort", String.valueOf(SocketUtils.findAvailableTcpPort(3000, 4000)));
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();

        try {
            quorumConfiguration.parseProperties(startupProperties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);

        new Thread() {
            public void run() {
                try {
                    zooKeeperServer.runFromConfig(configuration);
                } catch (IOException e) {
                    LOG.error("ZooKeeper Failed", e);
                }
            }
        }.start();

        return quorumConfiguration;
    }

    private BalancerConfiguration getBalancerConfiguration() {
        BalancerConfiguration balancerConfiguration = new BalancerConfiguration();
        balancerConfiguration.setMetricAssignmentPersisterType("FILE");
        File file = new File(System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID());
        file.deleteOnExit();
        balancerConfiguration.setAssignmentFile(file.getAbsolutePath());
        balancerConfiguration.setBalanceDelay(0);
        balancerConfiguration.setBalancePeriod(1000);
        balancerConfiguration.setPersistDelay(0);
        balancerConfiguration.setPersistDelay(1000);
        balancerConfiguration.setCheckServerHealthInterval(10);
        return  balancerConfiguration;
    }

    private void createTimelyHosts(BalancerConfiguration balancerConfiguration, int numHosts) {
        for (int port=1000, x=0; x < numHosts; x++, port+=10) {
            TimelyBalancedHost tbh = TimelyBalancedHost.of("localhost", port);
            tbh.setBalancerConfig(balancerConfiguration);
            timelyHosts.add(tbh);
            ServiceRegistry.registerService(curatorFramework, tbh);
        }
    }

    private void registerCallbacks(BalancedMetricResolver resolver) {
        resolver.registerCallback(new MetricAssignedCallback() {
            @Override
            public void onEvent(MetricAssignedEvent event) {
                LOG.debug(String.format("%s metric:%s lose:%s:%d gain:%s:%d reason:%s", event.getClass().getSimpleName(),
                        event.getMetric(),
                        event.getLosingHost() == null ? null: event.getLosingHost().getHost(),
                        event.getLosingHost() == null ? null: event.getLosingHost().getTcpPort(),
                        event.getGainingHost().getHost(), event.getGainingHost().getTcpPort(), event.getReason()));
            }
        });

        resolver.registerCallback(new MetricHostCallback() {
            @Override
            public void onEvent(MetricHostEvent event) {
                LOG.debug(String.format("%s host:%s:%d type:%s", event.getClass().getSimpleName(),
                        event.getTimelyBalancedHost().getHost(), event.getTimelyBalancedHost().getTcpPort(), event.getActionType()));
            }
        });

        resolver.registerCallback(new MetricBalanceCallback() {
            @Override
            public void onEvent(MetricBalanceEvent event) {
                LOG.debug(String.format("%s type:%s progress:%s reassigned:%d", event.getClass().getSimpleName(),
                        event.getBalanceType(), event.getProgressType(), event.getNumRessigned()));
            }
        });
    }

    private TimelyBalancedHost randomServerDown(TestHealthChecker healthChecker, int numHosts) {
        TimelyBalancedHost tbh = timelyHosts.get(r.nextInt(numHosts));
        LOG.debug("Marking server down:" + TimelyBalancedHost.toStringShort(tbh));
        healthChecker.serverDown(tbh);
        return tbh;
    }

    private TimelyBalancedHost randomServerToggle(TestHealthChecker healthChecker, int numHosts) {
        TimelyBalancedHost tbh = timelyHosts.get(r.nextInt(numHosts));
        LOG.debug("Marking server " + (tbh.isUp() ? "down:" : "up:") + TimelyBalancedHost.toStringShort(tbh));
        healthChecker.serverToggle(tbh);
        return tbh;
    }

    private TimelyBalancedHost randomServerUnregister(int numHosts) {
        TimelyBalancedHost tbh = timelyHosts.get(r.nextInt(numHosts));
        LOG.debug("Unregistering:" + TimelyBalancedHost.toStringShort(tbh));
        ServiceRegistry.unregisterService(curatorFramework, tbh);
        return tbh;
    }

    @Test
    public void test1() {
        LOG.debug("Configure test");
        int numHosts = 10;
        BalancerConfiguration balancerConfiguration = getBalancerConfiguration();
        createTimelyHosts(balancerConfiguration, numHosts);
        TestHealthChecker healthChecker = new TestHealthChecker(balancerConfiguration, timelyHosts);
        BalancedMetricResolver balancedMetricResolver = new BalancedMetricResolver(curatorFramework, balancerConfiguration, healthChecker);
        registerCallbacks(balancedMetricResolver);
        LOG.debug("Start balancedMetricResolver");
        balancedMetricResolver.start();
        long now = System.currentTimeMillis();
        int numMetrics = 100;
        for (int x=0; x < numMetrics; x++) {
            balancedMetricResolver.getHostPortKeyIngest(String.format("metric%03d", x));
        }

        LOG.debug("Begin monitoring");
        long cycle = 0;
        List<TimelyBalancedHost> serversUnregistered = new ArrayList<>();
        while (System.currentTimeMillis() < now + 15000) {
            try {
                Thread.sleep(100);
                for (int x=0; x < 10000; x++) {
                    int mNum = r.nextInt(numMetrics);
                    balancedMetricResolver.getHostPortKeyIngest(String.format("metric%03d", mNum));
                }
//                if (cycle % 100 == 0) {
//                    randomServerDown(healthChecker, numHosts);
//                }
//                if (cycle % 2 == 0) {
//                    randomServerToggle(healthChecker, numHosts);
//                }
                if (cycle % 100 == 0) {
                    serversUnregistered.add(randomServerUnregister(numHosts));
                }
                if (cycle % 500 == 0) {
                    serversUnregistered.stream().forEach(tbh -> ServiceRegistry.registerService(curatorFramework, tbh));
                    serversUnregistered.clear();
                }

            } catch (InterruptedException e) {

            }
            cycle++;
        }
        LOG.debug("Stop balancedMetricResolver");
        balancedMetricResolver.stop();
    }

    @Test
    public void test2() {
        LOG.debug("Configure test");
        int numHosts = 10;
        BalancerConfiguration balancerConfiguration = getBalancerConfiguration();
        createTimelyHosts(balancerConfiguration, numHosts);
        TestHealthChecker healthChecker = new TestHealthChecker(balancerConfiguration, timelyHosts);
        BalancedMetricResolver balancedMetricResolver = new BalancedMetricResolver(curatorFramework, balancerConfiguration, healthChecker);
        registerCallbacks(balancedMetricResolver);
        LOG.debug("Start balancedMetricResolver");
        balancedMetricResolver.start();
        long now = System.currentTimeMillis();
        int numMetrics = 100;
        for (int x=0; x < numMetrics; x++) {
            balancedMetricResolver.getHostPortKeyIngest(String.format("metric%03d", x));
        }

        LOG.debug("Begin monitoring");
        long cycle = 0;
        List<TimelyBalancedHost> serversUnregistered = new ArrayList<>();
        while (System.currentTimeMillis() < now + 15000) {
            try {
                Thread.sleep(100);
                for (int x=0; x < 10000; x++) {
                    int mNum = r.nextInt(numMetrics);
                    balancedMetricResolver.getHostPortKeyIngest(String.format("metric%03d", mNum));
                }
//                if (cycle % 100 == 0) {
//                    randomServerDown(healthChecker, numHosts);
//                }
//                if (cycle % 2 == 0) {
//                    randomServerToggle(healthChecker, numHosts);
//                }
                if (cycle % 100 == 0) {
                    serversUnregistered.add(randomServerUnregister(numHosts));
                }
                if (cycle % 500 == 0) {
                    serversUnregistered.stream().forEach(tbh -> ServiceRegistry.registerService(curatorFramework, tbh));
                    serversUnregistered.clear();
                }

            } catch (InterruptedException e) {

            }
            cycle++;
        }
        LOG.debug("Stop balancedMetricResolver");
        balancedMetricResolver.stop();
    }
}

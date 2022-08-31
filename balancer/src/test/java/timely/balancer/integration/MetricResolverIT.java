package timely.balancer.integration;

import static timely.Constants.SERVICE_DISCOVERY_PATH;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.x.discovery.ServiceCache;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.curator.x.discovery.details.ServiceCacheListener;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import timely.ServerDetails;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.test.TestHealthChecker;
import timely.balancer.test.TestMetricResolver;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class MetricResolverIT {

    private static final Logger log = LoggerFactory.getLogger(MetricResolverIT.class);

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private TestMetricResolver metricResolver;

    @Autowired
    private TestHealthChecker healthChecker;

    @Autowired
    private CuratorFramework curatorFramework;

    private ServiceDiscovery<ServerDetails> discovery;
    private ServiceCache<ServerDetails> serviceCache;

    @Before
    public void setup() throws Exception {
        discovery = ServiceDiscoveryBuilder.builder(ServerDetails.class).client(curatorFramework).basePath(SERVICE_DISCOVERY_PATH).build();
        discovery.start();
        serviceCache = discovery.serviceCacheBuilder().name("timely-server").build();
    }

    @Test
    public void ServiceRegistrationTest() {
        LinkedBlockingQueue<List<ServiceInstance<ServerDetails>>> updateQueue = getServiceUpdateQueue(serviceCache);
        registerService(curatorFramework, TimelyBalancedHost.of("host1", 5242, 5243, 5244, 5245, null));
        registerService(curatorFramework, TimelyBalancedHost.of("host1", 5342, 5343, 5344, 5345, null));
        registerService(curatorFramework, TimelyBalancedHost.of("host1", 5442, 5443, 5444, 5445, null));
        registerService(curatorFramework, TimelyBalancedHost.of("host1", 5542, 5543, 5544, 5545, null));
        try {
            List<ServiceInstance<ServerDetails>> currentUpdate;
            List<ServiceInstance<ServerDetails>> lastUpdate = null;
            do {
                currentUpdate = updateQueue.poll(200, TimeUnit.MILLISECONDS);
                if (currentUpdate != null) {
                    lastUpdate = currentUpdate;
                }
            } while (currentUpdate != null);
            Assert.assertNotNull(lastUpdate);
            Assert.assertEquals(4, lastUpdate.size());
        } catch (InterruptedException e) {
            Assert.fail("" + e.getMessage());
        }
        Assert.assertEquals(4, metricResolver.getServerList().size());
        Assert.assertEquals(4, healthChecker.getTimelyHosts().size());
    }

    public void registerService(CuratorFramework curatorFramework, TimelyBalancedHost server) {
        try {
            ServerDetails payload = new ServerDetails();
            payload.setHost(server.getHost());
            payload.setTcpPort(server.getTcpPort());
            payload.setHttpPort(server.getHttpPort());
            payload.setWsPort(server.getWsPort());
            payload.setUdpPort(server.getUdpPort());

            ServiceInstanceBuilder<ServerDetails> builder = ServiceInstance.builder();
            String serviceName = server.getHost() + ":" + server.getTcpPort();
            ServiceInstance<ServerDetails> serviceInstance = builder.id(serviceName).name("timely-server").address(server.getHost()).port(server.getTcpPort())
                            .payload(payload).build();

            ServiceDiscovery<ServerDetails> discovery = ServiceDiscoveryBuilder.builder(ServerDetails.class).client(curatorFramework)
                            .basePath(SERVICE_DISCOVERY_PATH).build();
            discovery.start();
            discovery.registerService(serviceInstance);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    private LinkedBlockingQueue<List<ServiceInstance<ServerDetails>>> getServiceUpdateQueue(ServiceCache<ServerDetails> serviceCache) {

        LinkedBlockingQueue<List<ServiceInstance<ServerDetails>>> updates = new LinkedBlockingQueue<>();
        try {

            ServiceCacheListener listener = new ServiceCacheListener() {

                @Override
                public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {

                }

                @Override
                public void cacheChanged() {
                    updates.add(serviceCache.getInstances());
                }
            };
            serviceCache.addListener(listener);
            serviceCache.start();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        return updates;
    }
}

package timely.balancer.resolver;

import static timely.Server.SERVICE_DISCOVERY_PATH;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.ServerDetails;
import timely.balancer.connection.TimelyBalancedHost;

public class ServiceRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceRegistry.class);

    public static void registerService(CuratorFramework curatorFramework, TimelyBalancedHost tbh) {
        try {
            try {
                Stat stat = curatorFramework.checkExists().forPath(SERVICE_DISCOVERY_PATH);
                if (stat == null) {
                    curatorFramework.create().creatingParentContainersIfNeeded().forPath(SERVICE_DISCOVERY_PATH);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }

            ServiceInstance<ServerDetails> serviceInstance = serviceInstance(tbh);
            ServiceDiscovery<ServerDetails> discovery = ServiceDiscoveryBuilder.builder(ServerDetails.class)
                    .client(curatorFramework).basePath(SERVICE_DISCOVERY_PATH).build();
            discovery.start();
            discovery.registerService(serviceInstance);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void unregisterService(CuratorFramework curatorFramework, TimelyBalancedHost tbh) {
        try {
            try {
                Stat stat = curatorFramework.checkExists().forPath(SERVICE_DISCOVERY_PATH);
                if (stat == null) {
                    curatorFramework.create().creatingParentContainersIfNeeded().forPath(SERVICE_DISCOVERY_PATH);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }

            ServiceInstance<ServerDetails> serviceInstance = serviceInstance(tbh);
            ServiceDiscovery<ServerDetails> discovery = ServiceDiscoveryBuilder.builder(ServerDetails.class)
                    .client(curatorFramework).basePath(SERVICE_DISCOVERY_PATH).build();
            discovery.start();
            discovery.unregisterService(serviceInstance);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private static ServiceInstance<ServerDetails> serviceInstance(TimelyBalancedHost config) throws Exception {
        ServerDetails serverDetails = new ServerDetails();
        serverDetails.setHost(config.getHost());
        serverDetails.setTcpPort(config.getTcpPort());
        serverDetails.setHttpPort(config.getHttpPort());
        serverDetails.setWsPort(config.getWsPort());
        serverDetails.setUdpPort(config.getUdpPort());

        ServiceInstanceBuilder<ServerDetails> builder = ServiceInstance.builder();
        String serviceName = config.getHost() + ":" + config.getTcpPort();
        ServiceInstance<ServerDetails> serviceInstance = builder.id(serviceName).name("timely-server")
                .address(config.getHost()).port(config.getTcpPort()).payload(serverDetails).build();
        return serviceInstance;
    }
}

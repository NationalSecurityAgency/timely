package timely.common.configuration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import javax.inject.Singleton;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MemoryUnit;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.accumulo.minicluster.ServerType;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
@ConditionalOnProperty(name = "timely.mini-accumulo.enabled", havingValue = "true")
@EnableConfigurationProperties({ZookeeperProperties.class, AccumuloProperties.class, MiniAccumuloProperties.class})
public class MiniAccumuloConfiguration {

    private static final Logger log = LoggerFactory.getLogger(MiniAccumuloConfiguration.class);

    @Bean(destroyMethod = "stop", name = "MiniAccumuloCluster")
    @Singleton
    public MiniAccumuloCluster miniAccumuloCluster(MiniAccumuloProperties miniAccumuloProperties, ZookeeperProperties zookeeperProperties,
                    AccumuloProperties accumuloProperties, ApplicationContext applicationContext) throws Exception {
        MiniAccumuloCluster miniAccumuloCluster;
        Path tempDirectory = Files.createTempDirectory("miniaccumulo");
        File tmp = tempDirectory.toFile();
        if (!tmp.canWrite()) {
            log.error("Unable to write to directory: " + tmp);
            SpringApplication.exit(applicationContext, () -> 0);
        }

        File accumuloDir = new File(tmp, "accumulo");
        if (!accumuloProperties.getUsername().equals("root")) {
            throw new IllegalArgumentException("Accumulo user for Standalone Timely should be root");
        }
        MiniAccumuloConfig macConfig = new MiniAccumuloConfig(accumuloDir, accumuloProperties.getPassword());
        macConfig.setInstanceName(accumuloProperties.getInstanceName());
        int zookeeperPort = 0;
        String zookeepers = zookeeperProperties.getServers();
        if (StringUtils.isNotBlank(zookeepers)) {
            String[] zkList = zookeepers.split(",");
            if (zkList != null) {
                String[] zkSplit = zkList[0].split(":");
                if (zkSplit != null && zkSplit.length >= 2) {
                    zookeeperPort = Integer.parseInt(zkSplit[1]);
                }
            }
        }

        macConfig.setZooKeeperPort(zookeeperPort);
        macConfig.setNumTservers(1);
        macConfig.setMemory(ServerType.TABLET_SERVER, 1, MemoryUnit.GIGABYTE);
        try {
            miniAccumuloCluster = new MiniAccumuloCluster(macConfig);
            log.info("Starting MiniAccumuloCluster");
            miniAccumuloCluster.start();
            if (zookeeperPort == 0) {
                // MiniAccumuloCluster chose an available port for zookeeper
                zookeeperProperties.setServers(miniAccumuloCluster.getZooKeepers());
                log.info("Using server {} for zookeeper", miniAccumuloCluster.getZooKeepers());
            }
            String instanceName = miniAccumuloCluster.getInstanceName();
            log.info("MiniAccumuloCluster started with zookeepers: {} and instance: {}", miniAccumuloCluster.getZooKeepers(), instanceName);

            try {
                log.info("MiniAccumuloCluster getting accumuloClient");
                AccumuloClient accumuloClient = Accumulo.newClient().from(miniAccumuloCluster.getClientProperties()).build();
                SecurityOperations securityOperations = accumuloClient.securityOperations();
                Authorizations rootAuths = miniAccumuloProperties.getRootAuthorizations();
                securityOperations.changeUserAuthorizations(accumuloProperties.getUsername(), rootAuths);
                log.info("MiniAccumuloCluster set user authorizations to: " + rootAuths);
            } catch (AccumuloException | AccumuloSecurityException e) {
                log.error("Error configuring root user", e);
                throw e;
            }

        } catch (IOException | InterruptedException e) {
            log.error("Error starting MiniAccumuloCluster: " + e.getMessage(), e);
            throw e;
        }
        return miniAccumuloCluster;
    }

    @Bean
    @Primary
    public AccumuloClient miniAccumuloClient(MiniAccumuloCluster miniAccumuloCluster) {
        return Accumulo.newClient().from(miniAccumuloCluster.getClientProperties()).build();
    }
}

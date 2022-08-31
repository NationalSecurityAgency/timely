package timely.test.configuration;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.Executors;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import timely.common.configuration.ZookeeperProperties;

@Configuration
@ConditionalOnProperty(name = "timely.in-memory-accumulo.enabled", havingValue = "true")
public class TestZookeeperConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TestZookeeperConfiguration.class);
    private static final int MAX_PORT = 100;
    private int port;

    private ZookeeperProperties zookeeperProperties;

    @Bean(name = "ZooKeeperServerMain", destroyMethod = "close")
    public ZooKeeperServerMain zooKeeperServerMain(ZookeeperProperties zookeeperProperties) throws Exception {
        this.zookeeperProperties = zookeeperProperties;
        this.port = getNextPort(2181);
        this.zookeeperProperties.setServers(String.format("localhost:%d", port));

        File tempDir;
        try {
            tempDir = Files.createTempDirectory("zookeeper_temp").toFile();
            tempDir.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create temp directory for mini accumulo cluster");
        }

        Properties properties = new Properties();
        properties.put("dataDir", tempDir.getAbsolutePath() + "/data");
        properties.put("clientPort", port);
        properties.put("skipACL", "true");
        properties.put("leaderServes", "yes");
        properties.put("4lw.commands.whitelist", "*");
        properties.put("admin.enableServer", "false");
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(properties);

        ZooKeeperServerMain zookeeper = new ZooKeeperServerMain();
        final ServerConfig config = new ServerConfig();
        config.readFrom(quorumPeerConfig);
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                zookeeper.runFromConfig(config);
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        });
        log.info("Started TestZookeeper on port {}", port);
        return zookeeper;
    }

    private int getNextPort(int start) {
        for (int port = start; port < start + MAX_PORT; ++port) {
            try {
                new ServerSocket(port).close();
                return port;
            } catch (IOException e) {

            }
        }
        return -1;
    }
}

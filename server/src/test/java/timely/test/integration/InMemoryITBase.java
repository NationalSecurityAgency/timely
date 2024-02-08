package timely.test.integration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import timely.Server;
import timely.auth.AuthCache;
import timely.configuration.Configuration;
import timely.store.MetricAgeOffIterator;
import timely.test.TestConfiguration;

/**
 * Base class for integration tests using in-memory-accumulo.
 */
public class InMemoryITBase {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryITBase.class);

    protected static final String ROOT_USER = "root";
    protected Configuration conf = null;
    protected AccumuloClient accumuloClient;
    protected Server s;
    private ZooKeeperServerMain zookeeper;
    private Future zooFuture;

    @BeforeClass
    public static void setupInMemoryAccumulo() throws Exception {

        InMemoryInstance instance = new InMemoryInstance();
        try {
            AccumuloClient accumuloClient = new InMemoryAccumuloClient(ROOT_USER, instance);
            accumuloClient.securityOperations().changeUserAuthorizations(accumuloClient.whoami(), new Authorizations("PUBLIC", "A", "B", "C"));
        } catch (AccumuloSecurityException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Before
    public void startupInMemoryBase() throws Exception {
        AuthCache.clear();
        clearTablesResetConf();
        InMemoryInstance instance = new InMemoryInstance();
        accumuloClient = new InMemoryAccumuloClient(ROOT_USER, instance);

        File tempDir;
        try {
            tempDir = Files.createTempDirectory("zookeeper_temp").toFile();
            tempDir.deleteOnExit();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create temp directory for mini accumulo cluster");
        }

        Properties properties = new Properties();
        properties.put("dataDir", tempDir.getAbsolutePath() + "/data");
        properties.put("clientPort", "2181");
        properties.put("skipACL", "true");
        properties.put("leaderServes", "yes");
        properties.put("4lw.commands.whitelist", "*");
        QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
        quorumPeerConfig.parseProperties(properties);

        zookeeper = new ZooKeeperServerMain();
        final ServerConfig config = new ServerConfig();
        config.readFrom(quorumPeerConfig);
        zooFuture = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                zookeeper.runFromConfig(config);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        });
    }

    protected void stopServer() {
        if (s != null) {
            s.setAccumuloClient(null);
            s.shutdown();
        }
    }

    @After
    public void shutdownInMemoryBase() throws Exception {
        if (zookeeper != null) {
            zookeeper.close();
        }

        if (zooFuture != null) {
            zooFuture.cancel(true);
        }
    }

    private void clearTablesResetConf() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        AccumuloClient accumuloClient = new InMemoryAccumuloClient(ROOT_USER, instance);
        accumuloClient.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    accumuloClient.tableOperations().delete(t);
                } catch (Exception e) {}
            }
        });
        // Reset configuration
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getAccumulo().setInstanceName(instance.getInstanceName());
        conf.getAccumulo().setZookeepers(instance.getZooKeepers());
        conf.getSecurity().getServerSsl().setUseOpenssl(false);
        conf.getSecurity().getServerSsl().setUseGeneratedKeypair(true);
        conf.getWebsocket().setFlushIntervalSeconds(TestConfiguration.WAIT_SECONDS);
        HashMap<String,Integer> ageOffSettings = new HashMap<>();
        ageOffSettings.put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 7);
        conf.setMetricAgeOffDays(ageOffSettings);
        // }
    }

    protected void deleteAccumuloEntries() throws Exception {
        InMemoryInstance instance = new InMemoryInstance();
        AccumuloClient accumuloClient = new InMemoryAccumuloClient(ROOT_USER, instance);
        accumuloClient.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    accumuloClient.tableOperations().deleteRows(t, new Text("#"), new Text("~"));
                } catch (Exception e) {}
            }
        });
    }

    protected void startServer() {
        try {
            s = new Server(conf, accumuloClient);
            s.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

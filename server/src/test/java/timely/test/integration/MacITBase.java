package timely.test.integration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Server;
import timely.auth.AuthCache;
import timely.configuration.Configuration;
import timely.store.MetricAgeOffIterator;
import timely.test.TestConfiguration;

/**
 * Base class for integration tests using mini accumulo cluster.
 */
public class MacITBase {

    private static final Logger LOG = LoggerFactory.getLogger(MacITBase.class);

    private static final File tempDir;

    protected static final String MAC_ROOT_USER = "root";
    protected static final String MAC_ROOT_PASSWORD = "secret";

    static {
        try {
            tempDir = Files.createTempDirectory("mac_temp").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Unable to create temp directory for mini accumulo cluster");
        }
        tempDir.deleteOnExit();
    }

    private static MiniAccumuloCluster mac = null;
    protected static Configuration conf = null;
    protected AccumuloClient accumuloClient;
    protected Server s;

    @BeforeClass
    public static void setupMiniAccumulo() throws Exception {
        if (null == mac) {
            final MiniAccumuloConfig macConfig = new MiniAccumuloConfig(tempDir, MAC_ROOT_PASSWORD);
            mac = new MiniAccumuloCluster(macConfig);
            mac.start();
        } else {
            LOG.info("Mini Accumulo already running.");
        }
    }

    @Before
    public void setupMacITBase() {
        AuthCache.clear();
        clearTablesResetConf();
        accumuloClient = mac.createAccumuloClient(MAC_ROOT_USER, new PasswordToken(MAC_ROOT_PASSWORD));
    }

    private void clearTablesResetConf() {
        try (AccumuloClient accumuloClient = mac.createAccumuloClient(MAC_ROOT_USER,
                new PasswordToken(MAC_ROOT_PASSWORD))) {
            accumuloClient.tableOperations().list().forEach(t -> {
                if (t.startsWith("timely")) {
                    try {
                        accumuloClient.tableOperations().delete(t);
                    } catch (Exception e) {
                    }
                }
            });
        }
        // Reset configuration
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getAccumulo().setInstanceName(mac.getInstanceName());
        conf.getAccumulo().setZookeepers(mac.getZooKeepers());
        conf.getSecurity().getServerSsl().setUseOpenssl(false);
        conf.getSecurity().getServerSsl().setUseGeneratedKeypair(true);
        conf.getWebsocket().setFlushIntervalSeconds(TestConfiguration.WAIT_SECONDS);
        HashMap<String, Integer> ageOffSettings = new HashMap<>();
        ageOffSettings.put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 7);
        conf.setMetricAgeOffDays(ageOffSettings);
    }

    protected void startServer() throws Exception {
        s = new Server(conf, accumuloClient);
        s.run();
    }

    protected void stopServer() {
        if (s != null) {
            s.shutdown();
        }
    }

    @After
    public void shutdown() {
        if (accumuloClient != null) {
            accumuloClient.close();
            accumuloClient = null;
        }

        if (s != null) {
            s.shutdown();
        }
    }
}

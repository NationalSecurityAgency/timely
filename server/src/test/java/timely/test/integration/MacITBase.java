package timely.test.integration;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.test.TestConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

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

    protected static MiniAccumuloCluster mac = null;
    protected static Configuration conf = null;

    @BeforeClass
    public static void setupMiniAccumulo() throws Exception {
        if (null == mac) {
            final MiniAccumuloConfig macConfig = new MiniAccumuloConfig(tempDir, MAC_ROOT_PASSWORD);
            mac = new MiniAccumuloCluster(macConfig);
            mac.start();
            conf = TestConfiguration.createMinimalConfigurationForTest();
            conf.getAccumulo().setInstanceName(mac.getInstanceName());
            conf.getAccumulo().setZookeepers(mac.getZooKeepers());
            conf.getSecurity().getSsl().setUseOpenssl(false);
            conf.getSecurity().getSsl().setUseGeneratedKeypair(true);
        } else {
            LOG.info("Mini Accumulo already running.");
        }
    }

    @Before
    public void clearTablesResetConf() throws Exception {
        Connector con = mac.getConnector(MAC_ROOT_USER, MAC_ROOT_PASSWORD);
        con.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    con.tableOperations().delete(t);
                } catch (Exception e) {
                }
            }
        });
        // Reset configuration
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getAccumulo().setInstanceName(mac.getInstanceName());
        conf.getAccumulo().setZookeepers(mac.getZooKeepers());
        conf.getSecurity().getSsl().setUseOpenssl(false);
        conf.getSecurity().getSsl().setUseGeneratedKeypair(true);
    }

}

package timely.test.integration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Server;
import timely.auth.AuthCache;
import timely.configuration.Configuration;
import timely.configuration.ServerSsl;
import timely.store.MetricAgeOffIterator;
import timely.test.TestConfiguration;

/**
 * Base class for integration tests using mini accumulo cluster.
 */
public class MacITBase {

    private static final Logger LOG = LoggerFactory.getLogger(MacITBase.class);

    protected static ServerSsl serverSsl = null;
    protected static File clientTrustStoreFile = null;
    protected static SslContext sslCtx = null;
    protected static SelfSignedCertificate serverCert = null;

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
            conf.getWebsocket().setFlushIntervalSeconds(TestConfiguration.WAIT_SECONDS);
            HashMap<String, Integer> ageOffSettings = new HashMap<>();
            ageOffSettings.put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 7);
            conf.setMetricAgeOffDays(ageOffSettings);
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
        conf.getSecurity().getServerSsl().setUseOpenssl(false);
        conf.getSecurity().getServerSsl().setUseGeneratedKeypair(true);
        conf.getWebsocket().setFlushIntervalSeconds(TestConfiguration.WAIT_SECONDS);
        HashMap<String, Integer> ageOffSettings = new HashMap<>();
        ageOffSettings.put(MetricAgeOffIterator.DEFAULT_AGEOFF_KEY, 7);
        conf.setMetricAgeOffDays(ageOffSettings);
    }

    @Before
    public void initializeAuthCache() {
        AuthCache.clear();
    }

    protected static void setupSSL() throws Exception {
        setupSSL(TestConfiguration.createMinimalConfigurationForTest(), false);
    }

    protected static void setupSSL(Configuration config, boolean twoWaySsl) throws Exception {
        setupSSL(config, twoWaySsl, null);
    }

    protected static void setupSSL(Configuration config, boolean twoWaySsl, SelfSignedCertificate serverCert)
            throws Exception {
        ServerSsl serverSsl = config.getSecurity().getServerSsl();
        boolean generateNewSsl = MacITBase.serverSsl == null || serverCert != null
                || !MacITBase.serverSsl.equals(serverSsl);

        if (generateNewSsl) {
            // new serverCert and trustStoreFile
            if (serverCert == null) {
                MacITBase.serverCert = new SelfSignedCertificate();
            } else {
                MacITBase.serverCert = serverCert;
            }
            MacITBase.clientTrustStoreFile = MacITBase.serverCert.certificate().getAbsoluteFile();
            MacITBase.serverSsl = serverSsl;
            MacITBase.serverSsl.setCertificateFile(MacITBase.serverCert.certificate().getAbsolutePath());
            MacITBase.serverSsl.setKeyFile(MacITBase.serverCert.privateKey().getAbsolutePath());
        }

        config.getSecurity().getServerSsl().setCertificateFile(MacITBase.serverSsl.getCertificateFile());
        config.getSecurity().getServerSsl().setKeyFile(MacITBase.serverSsl.getKeyFile());
        config.getSecurity().getServerSsl().setUseOpenssl(false);
        config.getSecurity().getServerSsl().setUseGeneratedKeypair(false);
        if (twoWaySsl) {
            // Needed for 2way SSL
            config.getSecurity().getServerSsl().setTrustStoreFile(MacITBase.serverSsl.getCertificateFile());
            config.getSecurity().setAllowAnonymousHttpAccess(false);
        } else {
            config.getSecurity().setAllowAnonymousHttpAccess(true);
        }
        if (generateNewSsl) {
            // new SslContext
            OneWaySSLBase.sslCtx = Server.createSSLContext(config);
        }
    }

    protected static SslContext getSslContext() {
        return MacITBase.sslCtx;
    }
}

package timely.test.integration;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import timely.Server;
import timely.auth.AuthCache;
import timely.configuration.Configuration;
import timely.test.TestConfiguration;

public class TwoWaySSLFailureIT extends TwoWaySSLBase {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static MiniAccumuloCluster mac = null;
    private static Configuration conf = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        temp.create();
        final MiniAccumuloConfig macConfig = new MiniAccumuloConfig(temp.newFolder("mac"), "secret");
        mac = new MiniAccumuloCluster(macConfig);
        mac.start();
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getAccumulo().setInstanceName(mac.getInstanceName());
        conf.getAccumulo().setZookeepers(mac.getZooKeepers());
        // This fqdn does not match what is in security.xml
        setupSSL(conf, true, new SelfSignedCertificate("CN=bad.example.com"));
    }

    @AfterClass
    public static void afterClass() throws Exception {
        mac.stop();
    }

    @Before
    public void setup() throws Exception {
        Connector con = mac.getConnector("root", "secret");
        con.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    con.tableOperations().delete(t);
                } catch (Exception e) {
                }
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        AuthCache.resetConfiguration();
    }

    @Test(expected = UnauthorizedUserException.class)
    public void testBasicAuthLoginFailure() throws Exception {
        final Server s = new Server(conf);
        s.run(getSslContext());
        try {
            String metrics = "https://localhost:54322/api/metrics";
            query(metrics);
        } finally {
            s.shutdown();
        }
    }

}

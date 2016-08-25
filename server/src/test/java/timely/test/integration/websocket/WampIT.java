package timely.test.integration.websocket;

import io.netty.handler.ssl.SslContextBuilder;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import timely.Configuration;
import timely.TestServer;
import timely.auth.AuthCache;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import ws.wamp.jawampa.Reply;
import ws.wamp.jawampa.WampClient;
import ws.wamp.jawampa.WampClientBuilder;
import ws.wamp.jawampa.connection.IWampConnectorProvider;
import ws.wamp.jawampa.transport.netty.NettyWampClientConnectorProvider;
import ws.wamp.jawampa.transport.netty.NettyWampConnectionConfig;

import java.util.concurrent.TimeUnit;

/**
 * Integration tests for the operations available over WAMP
 */
@Category(IntegrationTest.class)
public class WampIT {

    private static final Logger LOG = LoggerFactory.getLogger(WampIT.class);
    private static final Long TEST_TIME = System.currentTimeMillis();

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static MiniAccumuloCluster mac = null;
    private static Configuration conf = null;
    private static TestServer server = null;


    @BeforeClass
    public static void beforeClass() throws Exception {
        final MiniAccumuloConfig macConfig = new MiniAccumuloConfig(temp.newFolder("mac"), "secret");
        mac = new MiniAccumuloCluster(macConfig);
        mac.start();
        conf = TestConfiguration.createMinimalConfigurationForTest();
        conf.getAccumulo().setInstanceName(mac.getInstanceName());
        conf.getAccumulo().setZookeepers(mac.getZooKeepers());
        conf.getSecurity().getSsl().setUseOpenssl(false);
        conf.getSecurity().getSsl().setUseGeneratedKeypair(true);

        server = new TestServer(conf);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        server.shutdown();
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
        AuthCache.resetSessionMaxAge();
    }

    @Test
    public void testVersion() throws Exception {
        //final TestServer m = new TestServer(conf);

        final WampClient client;
        IWampConnectorProvider connectorProvider = new NettyWampClientConnectorProvider();
        NettyWampConnectionConfig connectionConfiguration = new NettyWampConnectionConfig.Builder()
                .withSslContext(SslContextBuilder.forClient().build())
                .build();

        WampClientBuilder builder = new WampClientBuilder();
        builder.withConnectorProvider(connectorProvider)
                .withConnectionConfiguration(connectionConfiguration)
                .withUri("wss://localhost:54323/wamp")
                .withRealm("default")
                .withInfiniteReconnects()
                .withReconnectInterval(5, TimeUnit.SECONDS);
        // Create a client through the builder. This will not immediately start
        // a connection attempt
        client = builder.build();
        client.open();

        Observable<Reply> oReply = client.call("timely.version");
        Reply reply = oReply.toBlocking().last();
        System.out.println(reply.toString());
    }


}

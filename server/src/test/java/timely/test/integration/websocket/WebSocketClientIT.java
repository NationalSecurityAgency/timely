package timely.test.integration.websocket;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslClientContext;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.http.client.HttpResponseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Server;
import timely.api.response.MetricResponse;
import timely.auth.AuthCache;
import timely.client.websocket.SubscriptionClientHandler;
import timely.clients.WebSocketClient;
import timely.serialize.JsonSerializer;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import timely.test.integration.OneWaySSLBase;

@SuppressWarnings("deprecation")
@Category(IntegrationTest.class)
public class WebSocketClientIT extends OneWaySSLBase {

    private final static Logger LOG = LoggerFactory.getLogger(WebSocketClientIT.class);
    private static final Long TEST_TIME = System.currentTimeMillis() - (240 * 1000);

    private static SSLContext sslCtx = null;
    private Server s = null;

    private void setupSslCtx() throws Exception {
        Assert.assertNotNull(clientTrustStoreFile);
        SslContextBuilder builder = SslContextBuilder.forClient();
        builder.applicationProtocolConfig(ApplicationProtocolConfig.DISABLED);
        builder.sslProvider(SslProvider.JDK);
        builder.trustManager(clientTrustStoreFile); // Trust the server cert
        SslContext ctx = builder.build();
        Assert.assertEquals(JdkSslClientContext.class, ctx.getClass());
        JdkSslContext jdk = (JdkSslContext) ctx;
        sslCtx = jdk.context();
    }

    @Before
    public void setup() throws Exception {
        Connector con = mac.getConnector("root", "secret");
        con.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E", "F"));
        s = new Server(conf);
        s.run();
        setupSslCtx();
    }

    @After
    public void tearDown() throws Exception {
        s.shutdown();
        AuthCache.resetSessionMaxAge();
    }

    public void testWorkflow(WebSocketClient client) throws Exception {
        // Add some data
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1",
        	"sys.cpu.user " + TEST_TIME + " 1.0 tag3=value3 rack=r2", 
        	"sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 rack=r1", 
        	"sys.cpu.idle " + (TEST_TIME + 2) + " 3.0 tag3=value3 tag4=value4 rack=r2");
		// @formatter:on

        // Latency in TestConfiguration is 2s, wait for it
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

        List<String> messages = new ArrayList<>();
        SubscriptionClientHandler handler = new SubscriptionClientHandler() {

            @Override
            public void onOpen(Session session, EndpointConfig config) {
                session.addMessageHandler(new MessageHandler.Whole<String>() {

                    @Override
                    public void onMessage(String message) {
                        messages.add(message);
                        LOG.debug("Message received on Websocket session {}: {}", session.getId(), message);
                    }
                });
            }
        };
        client.open(handler);
        client.addSubscription("sys.cpu.user", null, TEST_TIME, 0, 5000);
        sleepUninterruptibly(10, TimeUnit.SECONDS);

        Assert.assertEquals(2, messages.size());
        messages.forEach(m -> {
            try {
                MetricResponse metric = JsonSerializer.getObjectMapper().readValue(m, MetricResponse.class);
                Assert.assertEquals("sys.cpu.user", metric.getMetric());
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        });
    }

    @Test
    public void testClientAnonymousAccess() throws Exception {
        WebSocketClient client = new WebSocketClient(sslCtx, "localhost", 54322, 54323, false, null, null, false, 65536);
        testWorkflow(client);
    }

    @Test
    public void testClientBasicAuthAccess() throws Exception {
        WebSocketClient client = new WebSocketClient(sslCtx, "localhost", 54322, 54323, true, "test", "test1", false,
                65536);
        testWorkflow(client);
    }

    @Test(expected = HttpResponseException.class)
    public void testClientBasicAuthAccessFailure() throws Exception {
        WebSocketClient client = new WebSocketClient(sslCtx, "localhost", 54322, 54323, true, "test", "test2", false,
                65536);
        testWorkflow(client);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClientBasicAuthParameterMismatch() throws Exception {
        WebSocketClient client = new WebSocketClient(sslCtx, "localhost", 54322, 54323, true, "test", null, false,
                65536);
        testWorkflow(client);
    }

}

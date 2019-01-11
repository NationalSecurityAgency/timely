package timely.test.integration.client;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslClientContext;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
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
import timely.api.response.MetricResponses;
import timely.auth.AuthCache;
import timely.client.websocket.ClientHandler;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;
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

    public void testWorkflow(WebSocketSubscriptionClient client) throws Exception {
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
        ClientHandler handler = new ClientHandler() {

            @Override
            public void onOpen(Session session, EndpointConfig config) {
                session.addMessageHandler(String.class, new MessageHandler.Whole<String>() {

                    @Override
                    public void onMessage(String message) {
                        messages.add(message);
                        LOG.debug("Message received on Websocket session {}: {}", session.getId(), message);
                    }
                });
            }

            @Override
            public void onClose(Session session, CloseReason reason) {
                super.onClose(session, reason);
                try {
                    client.close();
                } catch (IOException e) {
                    Assert.fail("Error calling close on client: " + e.getMessage());
                }
            }

            @Override
            public void onError(Session session, Throwable error) {
                super.onError(session, error);
                try {
                    client.close();
                } catch (IOException e) {
                    Assert.fail("Error calling close on client: " + e.getMessage());
                }
            }
        };

        try {
            client.open(handler);
            client.addSubscription("sys.cpu.user", null, TEST_TIME, TEST_TIME + 1000, 5000);
            sleepUninterruptibly(2, TimeUnit.SECONDS);

            Assert.assertEquals(1, messages.size());
            MetricResponses responses = JsonSerializer.getObjectMapper().readValue(messages.get(0),
                    MetricResponses.class);
            responses.getResponses().forEach(metric -> {
                try {
                    Assert.assertTrue("sys.cpu.user".equals(metric.getMetric()) || metric.isComplete());
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            });
        } finally {
            client.close();
        }
    }

    @Test
    public void testClientAnonymousAccess() throws Exception {
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(sslCtx, "localhost", 54322, 54323, false,
                null, null, false, 65536);
        testWorkflow(client);
    }

    @Test
    public void testClientBasicAuthAccess() throws Exception {
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(sslCtx, "localhost", 54322, 54323, true,
                "test", "test1", false, 65536);
        testWorkflow(client);
    }

    @Test(expected = HttpResponseException.class)
    public void testClientBasicAuthAccessFailure() throws Exception {
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(sslCtx, "localhost", 54322, 54323, true,
                "test", "test2", false, 65536);
        testWorkflow(client);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testClientBasicAuthParameterMismatch() throws Exception {
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(sslCtx, "localhost", 54322, 54323, true,
                "test", null, false, 65536);
        testWorkflow(client);
    }

}

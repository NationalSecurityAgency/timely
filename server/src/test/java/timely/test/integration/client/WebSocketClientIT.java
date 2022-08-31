package timely.test.integration.client;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.Session;

import org.apache.accumulo.core.conf.ClientProperty;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import com.fasterxml.jackson.databind.ObjectMapper;

import timely.api.response.MetricResponses;
import timely.client.websocket.ClientHandler;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;
import timely.common.configuration.AccumuloProperties;
import timely.common.configuration.HttpProperties;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.WebsocketProperties;
import timely.common.configuration.ZookeeperProperties;
import timely.serialize.JsonSerializer;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import timely.test.TimelyServerTestRule;
import timely.test.integration.TwoWaySSLBase;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"twoWaySsl"})
public class WebSocketClientIT extends TwoWaySSLBase {

    private final static Logger log = LoggerFactory.getLogger(WebSocketClientIT.class);
    private static final Long TEST_TIME = ((System.currentTimeMillis() / 1000) * 1000) - (240 * 1000);

    @Autowired
    @Rule
    public TimelyServerTestRule testRule;

    @Autowired
    private SecurityProperties securityProperties;

    @Autowired
    private HttpProperties httpProperties;

    @Autowired
    private WebsocketProperties websocketProperties;

    @Autowired
    private AccumuloProperties accumuloProperties;

    @Autowired
    private ZookeeperProperties zookeeperProperties;

    @Autowired
    @Qualifier("outboundJDKSslContext")
    private SSLContext outboundSSLContext;

    @Before
    public void setup() {
        super.setup();
        try {
            final Properties properties = new Properties();
            properties.put(ClientProperty.INSTANCE_NAME.getKey(), accumuloProperties.getInstanceName());
            properties.put(ClientProperty.INSTANCE_ZOOKEEPERS.getKey(), zookeeperProperties.getServers());
            properties.put(ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT.getKey(), zookeeperProperties.getTimeout());
            properties.put(ClientProperty.AUTH_PRINCIPAL.getKey(), accumuloProperties.getUsername());
            properties.put(ClientProperty.AUTH_TOKEN.getKey(), accumuloProperties.getPassword());
            properties.put(ClientProperty.AUTH_TYPE.getKey(), "password");
            accumuloClient.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C", "D", "E", "F"));
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @After
    public void cleanup() {
        super.cleanup();
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
        final AtomicBoolean complete = new AtomicBoolean(false);
        ClientHandler handler = new ClientHandler() {

            private ObjectMapper objectMapper = JsonSerializer.getObjectMapper();

            @Override
            public void onOpen(Session session, EndpointConfig config) {
                session.addMessageHandler(String.class, message -> {
                    messages.add(message);
                    log.debug("Message received on Websocket session {}: {}", session.getId(), message);
                    try {
                        MetricResponses responses = objectMapper.readValue(message, MetricResponses.class);
                        responses.getResponses().forEach(r -> {
                            if (r.isComplete()) {
                                complete.set(true);
                            }
                        });
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
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
            long start = System.currentTimeMillis();
            while (complete.get() == false && System.currentTimeMillis() < start + TimeUnit.SECONDS.toMillis(60)) {
                sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }

            Assert.assertTrue("Complete should be true", complete.get());

            MetricResponses metricResponses = new MetricResponses();
            for (String m : messages) {
                MetricResponses responses = JsonSerializer.getObjectMapper().readValue(messages.get(0), MetricResponses.class);
                responses.getResponses().forEach(r -> metricResponses.addResponse(r));
            }

            metricResponses.getResponses().forEach(metric -> {
                try {
                    Assert.assertTrue("sys.cpu.user".equals(metric.getMetric()));
                } catch (Exception e) {
                    Assert.fail(e.getMessage());
                }
            });
        } finally {
            client.close();
        }
    }

    @Test
    public void testClientAuthAccess() throws Exception {
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(outboundSSLContext, "localhost", httpProperties.getPort(),
                        websocketProperties.getPort(), true, false, false, 65536);
        testWorkflow(client);
    }

    @Test
    public void testClientAnonymousAccess() throws Exception {
        WebSocketSubscriptionClient client = new WebSocketSubscriptionClient(outboundSSLContext, "localhost", httpProperties.getPort(),
                        websocketProperties.getPort(), false, false, false, 65536);
        try {
            securityProperties.setAllowAnonymousWsAccess(true);
            testWorkflow(client);
        } finally {
            securityProperties.setAllowAnonymousWsAccess(false);
        }
    }
}

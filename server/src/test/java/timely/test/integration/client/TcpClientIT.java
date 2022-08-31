package timely.test.integration.client;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static timely.test.TestConfiguration.WAIT_SECONDS;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
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

import timely.api.request.MetricRequest;
import timely.client.tcp.TcpClient;
import timely.model.Metric;
import timely.model.Tag;
import timely.test.IntegrationTest;
import timely.test.TestCaptureRequestHandler;
import timely.test.TimelyServerTestRule;
import timely.test.integration.OneWaySSLBase;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"oneWaySsl"})
public class TcpClientIT extends OneWaySSLBase {

    private static final Logger log = LoggerFactory.getLogger(TcpClientIT.class);
    private static final Long TEST_TIME = (System.currentTimeMillis() / 1000) * 1000;

    @Autowired
    @Rule
    public TimelyServerTestRule testRule;

    @Autowired
    @Qualifier("tcp")
    public TestCaptureRequestHandler tcpRequests;

    private String hostIp;
    private int tcpPort;

    @Before
    public void setup() {
        super.setup();
        hostIp = serverProperties.getIp();
        tcpPort = serverProperties.getTcpPort();
        tcpRequests.clear();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testPut() throws Exception {
        try (TcpClient client = new TcpClient(hostIp, tcpPort)) {
            client.open();
            client.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n");
            client.flush();
            while (1 != tcpRequests.getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, tcpRequests.getResponses().size());
            Assert.assertEquals(MetricRequest.class, tcpRequests.getResponses().get(0).getClass());
            final MetricRequest actual = (MetricRequest) tcpRequests.getResponses().get(0);
            // @formatter:off
            final MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            // @formatter:on
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testPutMultiple() throws Exception {

        try (TcpClient client = new TcpClient(hostIp, tcpPort)) {
            client.open();
            // @formatter:off
            client.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n"
                       + "put sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4\n");
            client.flush();
            while (2 != tcpRequests.getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, tcpRequests.getResponses().size());
            Assert.assertEquals(MetricRequest.class, tcpRequests.getResponses().get(0).getClass());
            MetricRequest actual = (MetricRequest) tcpRequests.getResponses().get(0);
            MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, tcpRequests.getResponses().get(1).getClass());
            actual = (MetricRequest) tcpRequests.getResponses().get(1);
            expected = new MetricRequest(
                    Metric.newBuilder()
                        .name("sys.cpu.idle")
                        .value(TEST_TIME + 1, 1.0D)
                        .tag(new Tag("tag3", "value3"))
                        .tag(new Tag("tag4", "value4"))
                        .build()
            );
            // @formatter:on
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testPutInvalidTimestamp() throws Exception {
        try (TcpClient client = new TcpClient(hostIp, tcpPort)) {
            client.open();
            client.write("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n");
            client.flush();
            sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
            Assert.assertEquals(0, tcpRequests.getCount());
        }
    }

    @Test
    public void testPersistence() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                   "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                   "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
        // @formatter:on
        sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        assertTrue(accumuloClient.namespaceOperations().exists("timely"));
        assertTrue(accumuloClient.tableOperations().exists("timely.metrics"));
        assertTrue(accumuloClient.tableOperations().exists("timely.meta"));
        int count = 0;
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", Authorizations.EMPTY)) {
            log.debug("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
        count = 0;
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.meta", Authorizations.EMPTY)) {
            log.debug("Meta entry: " + entry);
            count++;
        }
        assertEquals(10, count);
        // count w/out versioning iterator to make sure that the optimization
        // for writing is working
        accumuloClient.tableOperations().removeIterator("timely.meta", "vers", EnumSet.of(IteratorScope.scan));
        // wait for zookeeper propagation
        sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        count = 0;
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.meta", Authorizations.EMPTY)) {
            log.debug("Meta no vers iter: " + entry);
            count++;
        }
        assertEquals(15, count);
    }

    @Test
    public void testPersistenceWithVisibility() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                   "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4 viz=(A|B)",
                   "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 viz=(C&B)");
        // @formatter:on
        dataStore.flush();
        sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        int count = 0;
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner(timelyProperties.getMetricsTable(), Authorizations.EMPTY)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(2, count);
        count = 0;
        Authorizations auth1 = new Authorizations("A");
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner(timelyProperties.getMetricsTable(), auth1)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(4, count);
        count = 0;
        Authorizations auth2 = new Authorizations("B", "C");
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner(timelyProperties.getMetricsTable(), auth2)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
    }

    protected void put(String... lines) throws Exception {
        StringBuffer format = new StringBuffer();
        for (String line : lines) {
            format.append("put ");
            format.append(line);
            format.append("\n");
        }
        try (TcpClient client = new TcpClient(hostIp, tcpPort)) {
            client.open();
            client.write(format.toString());
            client.flush();
        }
    }
}

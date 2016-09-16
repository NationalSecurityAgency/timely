package timely.test.integration.tcp;

import com.google.flatbuffers.FlatBufferBuilder;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.Configuration;
import timely.Server;
import timely.TestServer;
import timely.api.request.MetricRequest;
import timely.api.request.VersionRequest;
import timely.auth.AuthCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.test.IntegrationTest;
import timely.test.integration.MacITBase;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static timely.test.TestConfiguration.WAIT_SECONDS;

/**
 * Integration tests for the operations available over the TCP transport
 */
@Category(IntegrationTest.class)
public class TimelyTcpIT extends MacITBase {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyTcpIT.class);
    private static final Long TEST_TIME = System.currentTimeMillis();

    @After
    public void tearDown() throws Exception {
        AuthCache.resetSessionMaxAge();
    }

    @Test
    public void testVersion() throws Exception {
        final TestServer m = new TestServer(conf);
        m.run();
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write("version\n");
            writer.flush();
            while (1 != m.getTcpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, m.getTcpRequests().getResponses().size());
            Assert.assertEquals(VersionRequest.class, m.getTcpRequests().getResponses().get(0).getClass());
            VersionRequest v = (VersionRequest) m.getTcpRequests().getResponses().get(0);
            Assert.assertEquals(VersionRequest.VERSION, v.getVersion());
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPut() throws Exception {
        final TestServer m = new TestServer(conf);
        m.run();
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n");
            writer.flush();
            while (1 != m.getTcpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, m.getTcpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, m.getTcpRequests().getResponses().get(0).getClass());
            final MetricRequest actual = (MetricRequest) m.getTcpRequests().getResponses().get(0);
            // @formatter:off
            final MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            // @formatter on
            Assert.assertEquals(expected, actual);
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPutMultiple() throws Exception {

        final TestServer m = new TestServer(conf);
        m.run();
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true)) {
            // @formatter:off
            writer.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n"
                       + "put sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4\n");
            writer.flush();
            while (2 != m.getTcpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, m.getTcpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, m.getTcpRequests().getResponses().get(0).getClass());
            MetricRequest actual = (MetricRequest) m.getTcpRequests().getResponses().get(0);
            MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, m.getTcpRequests().getResponses().get(1).getClass());
            actual = (MetricRequest) m.getTcpRequests().getResponses().get(1);
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

        } finally {
            m.shutdown();
        }
    }

    private int createMetric(FlatBufferBuilder builder, String name, long timestamp, double value,
            Map<String, String> tags) {
        int n = builder.createString(name);
        int[] t = new int[tags.size()];
        int i = 0;
        for (Entry<String, String> e : tags.entrySet()) {
            t[i] = timely.api.flatbuffer.Tag.createTag(builder, builder.createString(e.getKey()),
                    builder.createString(e.getValue()));
            i++;
        }
        return timely.api.flatbuffer.Metric.createMetric(builder, n, timestamp, value,
                timely.api.flatbuffer.Metric.createTagsVector(builder, t));
    }

    @Test
    public void testPutMultipleBinary() throws Exception {

        FlatBufferBuilder builder = new FlatBufferBuilder(1);

        int[] metric = new int[2];
        Map<String, String> t = new HashMap<>();
        t.put("tag1", "value1");
        t.put("tag2", "value2");
        metric[0] = createMetric(builder, "sys.cpu.user", TEST_TIME, 1.0D, t);
        t = new HashMap<>();
        t.put("tag3", "value3");
        t.put("tag4", "value4");
        metric[1] = createMetric(builder, "sys.cpu.idle", TEST_TIME + 1, 1.0D, t);

        int metricVector = timely.api.flatbuffer.Metrics.createMetricsVector(builder, metric);

        timely.api.flatbuffer.Metrics.startMetrics(builder);
        timely.api.flatbuffer.Metrics.addMetrics(builder, metricVector);
        int metrics = timely.api.flatbuffer.Metrics.endMetrics(builder);
        timely.api.flatbuffer.Metrics.finishMetricsBuffer(builder, metrics);

        ByteBuffer binary = builder.dataBuffer();
        byte[] data = new byte[binary.remaining()];
        binary.get(data, 0, binary.remaining());
        LOG.debug("Sending {} bytes", data.length);

        final TestServer m = new TestServer(conf);
        m.run();
        try (Socket sock = new Socket("127.0.0.1", 54321);) {
            sock.getOutputStream().write(data);
            sock.getOutputStream().flush();
            while (2 != m.getTcpRequests().getCount()) {
                LOG.debug("Thread sleeping");
                Thread.sleep(5);
            }
            Assert.assertEquals(2, m.getTcpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, m.getTcpRequests().getResponses().get(0).getClass());
            // @formatter:off
            MetricRequest actual = (MetricRequest) m.getTcpRequests().getResponses().get(0);
            MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, m.getTcpRequests().getResponses().get(1).getClass());
            actual = (MetricRequest) m.getTcpRequests().getResponses().get(1);
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

        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPutInvalidTimestamp() throws Exception {
        final TestServer m = new TestServer(conf);
        m.run();
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);
                BufferedReader reader = new BufferedReader(new InputStreamReader(sock.getInputStream()));) {
            writer.write("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n");
            writer.flush();
            sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
            Assert.assertEquals(0, m.getTcpRequests().getCount());
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPersistence() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4");
            sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        } finally {
            s.shutdown();
        }
        final ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        final Connector connector = inst.getConnector("root", new PasswordToken("secret".getBytes(UTF_8)));
        assertTrue(connector.namespaceOperations().exists("timely"));
        assertTrue(connector.tableOperations().exists("timely.metrics"));
        assertTrue(connector.tableOperations().exists("timely.meta"));
        int count = 0;
        for (final Entry<Key, Value> entry : connector.createScanner("timely.metrics", Authorizations.EMPTY)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
        count = 0;
        for (final Entry<Key, Value> entry : connector.createScanner("timely.meta", Authorizations.EMPTY)) {
            LOG.info("Meta entry: " + entry);
            count++;
        }
        assertEquals(10, count);
        // count w/out versioning iterator to make sure that the optimization
        // for writing is working
        connector.tableOperations().removeIterator("timely.meta", "vers", EnumSet.of(IteratorScope.scan));
        // wait for zookeeper propagation
        sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        count = 0;
        for (final Entry<Key, Value> entry : connector.createScanner("timely.meta", Authorizations.EMPTY)) {
            LOG.info("Meta no vers iter: " + entry);
            count++;
        }
        assertEquals(10, count);
    }

    @Test
    public void testPersistenceWithVisibility() throws Exception {
        final Server s = new Server(conf);
        s.run();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4 viz=(a|b)", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 viz=(c&b)");
            sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        } finally {
            s.shutdown();
        }
        final ZooKeeperInstance inst = new ZooKeeperInstance(mac.getClientConfig());
        final Connector connector = inst.getConnector("root", new PasswordToken("secret".getBytes(UTF_8)));
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations("a", "b", "c"));

        int count = 0;
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.metrics", Authorizations.EMPTY)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(2, count);
        count = 0;
        Authorizations auth1 = new Authorizations("a");
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.metrics", auth1)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(4, count);
        count = 0;
        Authorizations auth2 = new Authorizations("b", "c");
        for (final Map.Entry<Key, Value> entry : connector.createScanner("timely.metrics", auth2)) {
            LOG.info("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
    }

    private void put(String... lines) throws Exception {
        StringBuffer format = new StringBuffer();
        for (String line : lines) {
            format.append("put ");
            format.append(line);
            format.append("\n");
        }
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write(format.toString());
            writer.flush();
        }
    }

}

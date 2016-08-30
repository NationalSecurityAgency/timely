package timely.test.integration.udp;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.Server;
import timely.TestServer;
import timely.api.model.Metric;
import timely.api.model.Tag;
import timely.auth.AuthCache;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;

import com.google.flatbuffers.FlatBufferBuilder;

/**
 * Integration tests for the operations available over the UDP transport
 */
@Category(IntegrationTest.class)
public class TimelyUdpIT {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyUdpIT.class);
    private static final Long TEST_TIME = System.currentTimeMillis();

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static MiniAccumuloCluster mac = null;
    private static Configuration conf = null;

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
        AuthCache.resetSessionMaxAge();
    }

    @Test
    public void testPut() throws Exception {
        final TestServer m = new TestServer(conf);
        m.run();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket();) {
            packet.setData(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n").getBytes(UTF_8));
            sock.send(packet);
            while (1 != m.getUdpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, m.getUdpRequests().getResponses().size());
            Assert.assertEquals(Metric.class, m.getUdpRequests().getResponses().get(0).getClass());
            final Metric actual = (Metric) m.getUdpRequests().getResponses().get(0);
            final Metric expected = new Metric();
            expected.setMetric("sys.cpu.user");
            expected.setTimestamp(TEST_TIME);
            expected.setValue(1.0);
            final List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("tag1", "value1"));
            tags.add(new Tag("tag2", "value2"));
            expected.setTags(tags);
            Assert.assertEquals(expected, actual);
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPutMultiple() throws Exception {

        final TestServer m = new TestServer(conf);
        m.run();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket();) {
            packet.setData(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n" + "put sys.cpu.idle "
                    + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4\n").getBytes(UTF_8));
            sock.send(packet);
            while (2 != m.getUdpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, m.getUdpRequests().getResponses().size());
            Assert.assertEquals(Metric.class, m.getUdpRequests().getResponses().get(0).getClass());
            Metric actual = (Metric) m.getUdpRequests().getResponses().get(0);
            Metric expected = new Metric();
            expected.setMetric("sys.cpu.user");
            expected.setTimestamp(TEST_TIME);
            expected.setValue(1.0);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("tag1", "value1"));
            tags.add(new Tag("tag2", "value2"));
            expected.setTags(tags);
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(Metric.class, m.getUdpRequests().getResponses().get(1).getClass());
            actual = (Metric) m.getUdpRequests().getResponses().get(1);
            expected = new Metric();
            expected.setMetric("sys.cpu.idle");
            expected.setTimestamp(TEST_TIME + 1);
            expected.setValue(1.0);
            tags = new ArrayList<>();
            tags.add(new Tag("tag3", "value3"));
            tags.add(new Tag("tag4", "value4"));
            expected.setTags(tags);
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
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket();) {
            packet.setData(data);
            sock.send(packet);
            while (2 != m.getUdpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, m.getUdpRequests().getResponses().size());
            Assert.assertEquals(Metric.class, m.getUdpRequests().getResponses().get(0).getClass());
            Metric actual = (Metric) m.getUdpRequests().getResponses().get(0);
            Metric expected = new Metric();
            expected.setMetric("sys.cpu.user");
            expected.setTimestamp(TEST_TIME);
            expected.setValue(1.0);
            List<Tag> tags = new ArrayList<>();
            tags.add(new Tag("tag1", "value1"));
            tags.add(new Tag("tag2", "value2"));
            expected.setTags(tags);
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(Metric.class, m.getUdpRequests().getResponses().get(1).getClass());
            actual = (Metric) m.getUdpRequests().getResponses().get(1);
            expected = new Metric();
            expected.setMetric("sys.cpu.idle");
            expected.setTimestamp(TEST_TIME + 1);
            expected.setValue(1.0);
            tags = new ArrayList<>();
            tags.add(new Tag("tag3", "value3"));
            tags.add(new Tag("tag4", "value4"));
            expected.setTags(tags);
            Assert.assertEquals(expected, actual);

        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPutInvalidTimestamp() throws Exception {
        final TestServer m = new TestServer(conf);
        m.run();
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket();) {
            packet.setData(("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n").getBytes(UTF_8));
            sock.send(packet);
            sleepUninterruptibly(1, TimeUnit.SECONDS);
            Assert.assertEquals(0, m.getUdpRequests().getCount());
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
            sleepUninterruptibly(5, TimeUnit.SECONDS);
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
        sleepUninterruptibly(3, TimeUnit.SECONDS);
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
            sleepUninterruptibly(5, TimeUnit.SECONDS);
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
        InetSocketAddress address = new InetSocketAddress("127.0.0.1", 54325);
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), 54325);
        try (DatagramSocket sock = new DatagramSocket();) {
            packet.setData(format.toString().getBytes(UTF_8));
            sock.send(packet);
        }
    }

}

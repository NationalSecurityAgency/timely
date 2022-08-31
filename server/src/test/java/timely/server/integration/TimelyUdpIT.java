package timely.server.integration;

import static java.nio.charset.StandardCharsets.UTF_8;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import com.google.flatbuffers.FlatBufferBuilder;

import timely.api.request.MetricRequest;
import timely.client.udp.UdpClient;
import timely.model.Metric;
import timely.model.Tag;
import timely.server.test.TestCaptureRequestHandler;
import timely.server.test.TestConfiguration;
import timely.server.test.TestDataStore;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

/**
 * Integration tests for the operations available over the UDP transport
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class TimelyUdpIT extends ITBase {

    private static final Long TEST_TIME = (System.currentTimeMillis() / 1000) * 1000;

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    @Qualifier("udp")
    public TestCaptureRequestHandler udpRequests;

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
        udpRequests.clear();
    }

    @Test
    public void testPut() throws Exception {
        InetSocketAddress address = new InetSocketAddress(serverProperties.getIp(), serverProperties.getUdpPort());
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), serverProperties.getUdpPort());
        try (DatagramSocket sock = new DatagramSocket()) {
            // @formatter:off
            packet.setData(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n").getBytes(UTF_8));
            sock.send(packet);
            long start = System.currentTimeMillis();
            while (1 != udpRequests.getCount()) {
                Thread.sleep(5);
                if ((System.currentTimeMillis() - start) > (TestConfiguration.WAIT_SECONDS * 1000)) {
                    Assert.fail("Failed to receive UDP updates in " + TestConfiguration.WAIT_SECONDS + " seconds");
                }
            }
            Assert.assertEquals(1, udpRequests.getResponses().size());
            Assert.assertEquals(MetricRequest.class, udpRequests.getResponses().get(0).getClass());
            final MetricRequest actual = (MetricRequest) udpRequests.getResponses().get(0);
            final MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);
            // @formatter:on
        }
    }

    @Test
    public void testPutMultiple() throws Exception {

        InetSocketAddress address = new InetSocketAddress(serverProperties.getIp(), serverProperties.getUdpPort());
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), serverProperties.getUdpPort());
        // @formatter:off
        try (DatagramSocket sock = new DatagramSocket()) {
            packet.setData(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n"
                          + "put sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4\n").getBytes(UTF_8));
            sock.send(packet);
            long start = System.currentTimeMillis();
            while (2 != udpRequests.getCount()) {
                Thread.sleep(5);
                if ((System.currentTimeMillis() - start) > (TestConfiguration.WAIT_SECONDS * 1000)) {
                    Assert.fail("Failed to receive UDP updates in " + TestConfiguration.WAIT_SECONDS + " seconds");
                }
            }
            Assert.assertEquals(2, udpRequests.getResponses().size());
            Assert.assertEquals(MetricRequest.class, udpRequests.getResponses().get(0).getClass());
            MetricRequest actual = (MetricRequest) udpRequests.getResponses().get(0);
            MetricRequest expected = new MetricRequest (
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, udpRequests.getResponses().get(1).getClass());
            actual = (MetricRequest) udpRequests.getResponses().get(1);
            expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.idle")
                            .value(TEST_TIME + 1, 1.0D)
                            .tag(new Tag("tag3", "value3"))
                            .tag(new Tag("tag4", "value4"))
                            .build()
            );
            Assert.assertEquals(expected, actual);
            // @formatter:on
        }
    }

    private int createMetric(FlatBufferBuilder builder, String name, long timestamp, double value, Map<String,String> tags) {
        int n = builder.createString(name);
        int[] t = new int[tags.size()];
        int i = 0;
        for (Entry<String,String> e : tags.entrySet()) {
            t[i] = timely.api.flatbuffer.Tag.createTag(builder, builder.createString(e.getKey()), builder.createString(e.getValue()));
            i++;
        }
        return timely.api.flatbuffer.Metric.createMetric(builder, n, timestamp, value, timely.api.flatbuffer.Metric.createTagsVector(builder, t));
    }

    @Test
    public void testPutMultipleBinary() throws Exception {

        FlatBufferBuilder builder = new FlatBufferBuilder(1);

        int[] metric = new int[2];
        Map<String,String> t = new HashMap<>();
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
        log.debug("Sending {} bytes", data.length);

        InetSocketAddress address = new InetSocketAddress(serverProperties.getIp(), serverProperties.getUdpPort());
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), serverProperties.getUdpPort());
        try (DatagramSocket sock = new DatagramSocket()) {
            packet.setData(data);
            sock.send(packet);
            long start = System.currentTimeMillis();
            while (2 != udpRequests.getCount()) {
                Thread.sleep(5);
                if ((System.currentTimeMillis() - start) > (TestConfiguration.WAIT_SECONDS * 1000)) {
                    Assert.fail("Failed to receive UDP updates in " + TestConfiguration.WAIT_SECONDS + " seconds");
                }
            }
            Assert.assertEquals(2, udpRequests.getResponses().size());
            Assert.assertEquals(MetricRequest.class, udpRequests.getResponses().get(0).getClass());
            // @formatter:off
            MetricRequest actual = (MetricRequest) udpRequests.getResponses().get(0);
            MetricRequest expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.user")
                            .value(TEST_TIME, 1.0D)
                            .tag(new Tag("tag1", "value1"))
                            .tag(new Tag("tag2", "value2"))
                            .build()
            );
            Assert.assertEquals(expected, actual);

            Assert.assertEquals(MetricRequest.class, udpRequests.getResponses().get(1).getClass());
            actual = (MetricRequest) udpRequests.getResponses().get(1);
            expected = new MetricRequest(
                    Metric.newBuilder()
                            .name("sys.cpu.idle")
                            .value(TEST_TIME + 1, 1.0D)
                            .tag(new Tag("tag3", "value3"))
                            .tag(new Tag("tag4", "value4"))
                            .build()
            );
            Assert.assertEquals(expected, actual);
            // @formatter:on

        }
    }

    @Test
    public void testPutInvalidTimestamp() throws Exception {

        InetSocketAddress address = new InetSocketAddress(serverProperties.getIp(), serverProperties.getUdpPort());
        DatagramPacket packet = new DatagramPacket("".getBytes(UTF_8), 0, 0, address.getAddress(), serverProperties.getUdpPort());
        try (DatagramSocket sock = new DatagramSocket();) {
            packet.setData(("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n").getBytes(UTF_8));
            sock.send(packet);
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            Assert.assertEquals(0, udpRequests.getCount());
        }
    }

    @Test
    public void testPersistence() throws Exception {
        // @formatter:off
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2",
                   "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                   "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
        // @formatter:on
        dataStore.flush();
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        assertTrue(accumuloClient.namespaceOperations().exists("timely"));
        assertTrue(accumuloClient.tableOperations().exists(timelyProperties.getMetricsTable()));
        assertTrue(accumuloClient.tableOperations().exists(timelyProperties.getMetaTable()));
        dataStore.flush();
        final AtomicInteger entryCount = new AtomicInteger(0);
        accumuloClient.createScanner(timelyProperties.getMetricsTable(), Authorizations.EMPTY).forEach(e -> {
            final double value = ByteBuffer.wrap(e.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            entryCount.getAndIncrement();
        });
        assertEquals(6, entryCount.get());
        entryCount.set(0);
        final List<Key> keys = new ArrayList<>();

        accumuloClient.createScanner(timelyProperties.getMetaTable(), Authorizations.EMPTY).forEach(e -> {
            entryCount.getAndIncrement();
            keys.add(e.getKey());
        });
        if (entryCount.get() != 10) {
            printAllAccumuloEntries();
        }
        assertEquals(10, entryCount.get());
        // count w/out versioning iterator to make sure that the optimization
        // for writing is working
        accumuloClient.tableOperations().removeIterator("timely.meta", "vers", EnumSet.of(IteratorScope.scan));
        // wait for zookeeper propagation
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        assertEquals(15, accumuloClient.createScanner("timely.meta", Authorizations.EMPTY).stream().count());
    }

    @Test
    public void testPersistenceWithVisibility() throws Exception {
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4 viz=(a|b)",
                        "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 viz=(c&b)");
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        accumuloClient.securityOperations().changeUserAuthorizations("root", new Authorizations("a", "b", "c"));

        int count = 0;
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", Authorizations.EMPTY)) {
            log.debug("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(2, count);
        count = 0;
        Authorizations auth1 = new Authorizations("a");
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", auth1)) {
            log.debug("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(4, count);
        count = 0;
        Authorizations auth2 = new Authorizations("b", "c");
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", auth2)) {
            log.debug("Entry: " + entry);
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
    }

    protected void put(String... lines) throws Exception {
        final CountDownLatch PUT_REQUESTS = new CountDownLatch(lines.length);
        TestDataStore.StoreCallback storeCallback = () -> PUT_REQUESTS.countDown();
        try {
            dataStore.addStoreCallback(storeCallback);
            StringBuffer format = new StringBuffer();
            for (String line : lines) {
                format.append("put ");
                format.append(line);
                format.append("\n");
            }
            try (UdpClient client = new UdpClient(serverProperties.getIp(), serverProperties.getUdpPort())) {
                client.open();
                client.write(format.toString());
            }
            PUT_REQUESTS.await(5, TimeUnit.SECONDS);
        } finally {
            dataStore.removeStoreCallback(storeCallback);
        }
    }
}

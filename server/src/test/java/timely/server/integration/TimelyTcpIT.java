package timely.server.integration;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static timely.server.test.TestConfiguration.WAIT_SECONDS;

import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
import timely.api.request.VersionRequest;
import timely.model.Metric;
import timely.model.Tag;
import timely.server.test.TestCaptureRequestHandler;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

/**
 * Integration tests for the operations available over the TCP transport
 */
@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class TimelyTcpIT extends ITBase {

    private static final Long TEST_TIME = ITBase.roundTimestampToLastHour(System.currentTimeMillis());

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    @Qualifier("tcp")
    public TestCaptureRequestHandler tcpRequests;

    @Before
    public void setup() {
        super.setup();
        tcpRequests.clear();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testVersion() throws Exception {
        try (Socket sock = new Socket(serverProperties.getIp(), serverProperties.getTcpPort());
                        PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write("version\n");
            writer.flush();
            while (1 != tcpRequests.getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, tcpRequests.getResponses().size());
            Assert.assertEquals(VersionRequest.class, tcpRequests.getResponses().get(0).getClass());
            VersionRequest v = (VersionRequest) tcpRequests.getResponses().get(0);
            Assert.assertEquals(VersionRequest.VERSION, v.getVersion());
        }
    }

    @Test
    public void testPut() throws Exception {
        try (Socket sock = new Socket(serverProperties.getIp(), serverProperties.getTcpPort());
                        PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n");
            writer.flush();
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
            // @formatter on
            Assert.assertEquals(expected, actual);
        }
    }

    @Test
    public void testPutMultiple() throws Exception {

        try (Socket sock = new Socket(serverProperties.getIp(), serverProperties.getTcpPort());
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true)) {
            // @formatter:off
            writer.write("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n"
                       + "put sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4\n");
            writer.flush();
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

        try (Socket sock = new Socket(serverProperties.getIp(), serverProperties.getTcpPort())) {
            sock.getOutputStream().write(data);
            sock.getOutputStream().flush();
            while (2 != tcpRequests.getCount()) {
                log.debug("Thread sleeping");
                Thread.sleep(5);
            }
            Assert.assertEquals(2, tcpRequests.getResponses().size());
            Assert.assertEquals(MetricRequest.class, tcpRequests.getResponses().get(0).getClass());
            // @formatter:off
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
        try (Socket sock = new Socket(serverProperties.getIp(), serverProperties.getTcpPort());
                        PrintWriter writer = new PrintWriter(sock.getOutputStream(), true)) {
            writer.write("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n");
            writer.flush();
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
        dataStore.flush();
        sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        assertTrue(accumuloClient.namespaceOperations().exists("timely"));
        assertTrue(accumuloClient.tableOperations().exists("timely.metrics"));
        assertTrue(accumuloClient.tableOperations().exists("timely.meta"));
        AtomicLong count = new AtomicLong(0);
        accumuloClient.createScanner("timely.metrics", Authorizations.EMPTY).forEach((k, v) -> {
            final double value = ByteBuffer.wrap(v.get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count.incrementAndGet();
        });
        assertEquals(6, count.get());
        assertEquals(10, accumuloClient.createScanner("timely.meta", Authorizations.EMPTY).stream().count());
        // count w/out versioning iterator to make sure that the optimization
        // for writing is working
        accumuloClient.tableOperations().removeIterator("timely.meta", "vers", EnumSet.of(IteratorScope.scan));
        // wait for zookeeper propagation
        sleepUninterruptibly(WAIT_SECONDS, TimeUnit.SECONDS);
        assertEquals(15, accumuloClient.createScanner("timely.meta", Authorizations.EMPTY).stream().count());
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
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", Authorizations.EMPTY)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(2, count);
        count = 0;
        Authorizations auth1 = new Authorizations("A");
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", auth1)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(4, count);
        count = 0;
        Authorizations auth2 = new Authorizations("B", "C");
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", auth2)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
    }
}

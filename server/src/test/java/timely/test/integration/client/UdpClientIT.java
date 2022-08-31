package timely.test.integration.client;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import timely.api.request.MetricRequest;
import timely.client.udp.UdpClient;
import timely.model.Metric;
import timely.model.Tag;
import timely.test.IntegrationTest;
import timely.test.TestCaptureRequestHandler;
import timely.test.TestConfiguration;
import timely.test.TimelyServerTestRule;
import timely.test.integration.ITBase;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class UdpClientIT extends ITBase {

    private static final Logger log = LoggerFactory.getLogger(UdpClientIT.class);
    private static final Long TEST_TIME = (System.currentTimeMillis() / 1000) * 1000;

    @Autowired
    @Rule
    public TimelyServerTestRule testRule;

    @Autowired
    @Qualifier("udp")
    public TestCaptureRequestHandler udpRequests;

    @Before
    public void setuo() {
        super.setup();
        udpRequests.clear();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test
    public void testPut() throws Exception {

        try (UdpClient client = new UdpClient(serverProperties.getIp(), serverProperties.getUdpPort())) {
            client.open();
            client.write(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n"));
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

        try (UdpClient client = new UdpClient(serverProperties.getIp(), serverProperties.getUdpPort())) {
            client.open();
            client.write(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n" + "put sys.cpu.idle " + (TEST_TIME + 1)
                            + " 1.0 tag3=value3 tag4=value4\n"));
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
            // @formatter:off
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

    @Test
    public void testPutInvalidTimestamp() throws IOException {
        try (UdpClient client = new UdpClient(serverProperties.getIp(), serverProperties.getUdpPort())) {
            client.open();
            client.write(("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n"));
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            Assert.assertEquals(0, udpRequests.getCount());
        }
    }

    @Test
    public void testPersistence() throws Exception {
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                        "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        dataStore.flush();
        assertTrue(accumuloClient.namespaceOperations().exists("timely"));
        assertTrue(accumuloClient.tableOperations().exists(timelyProperties.getMetricsTable()));
        assertTrue(accumuloClient.tableOperations().exists(timelyProperties.getMetaTable()));
        final AtomicInteger entryCount = new AtomicInteger(0);
        accumuloClient.createScanner(timelyProperties.getMetricsTable(), Authorizations.EMPTY).forEach(e -> {
            final double value = ByteBuffer.wrap(e.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            entryCount.getAndIncrement();
        });
        assertEquals(6, entryCount.get());
        entryCount.set(0);
        accumuloClient.createScanner(timelyProperties.getMetaTable(), Authorizations.EMPTY).forEach(e -> {
            entryCount.getAndIncrement();
        });
        if (entryCount.get() != 10) {
            printAllAccumuloEntries();
        }
        assertEquals(10, entryCount.get());
        // count w/out versioning iterator to make sure that the optimization
        // for writing is working
        accumuloClient.tableOperations().removeIterator(timelyProperties.getMetaTable(), "vers", EnumSet.of(IteratorScope.scan));
        // wait for zookeeper propagation
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        entryCount.set(0);
        accumuloClient.createScanner(timelyProperties.getMetaTable(), Authorizations.EMPTY).forEach(e -> {
            entryCount.getAndIncrement();
        });
        assertEquals(15, entryCount.get());
    }

    @Test
    public void testPersistenceWithVisibility() throws Exception {
        put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4 viz=(a|b)",
                        "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4 viz=(c&b)");
        sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
        accumuloClient.securityOperations().changeUserAuthorizations("root", new Authorizations("a", "b", "c"));

        int count = 0;
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", Authorizations.EMPTY)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(2, count);
        count = 0;
        Authorizations auth1 = new Authorizations("a");
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", auth1)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(4, count);
        count = 0;
        Authorizations auth2 = new Authorizations("b", "c");
        for (final Map.Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", auth2)) {
            final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
            assertEquals(1.0, value, 1e-9);
            count++;
        }
        assertEquals(6, count);
    }

    private void put(String... lines) throws IOException {
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
    }
}

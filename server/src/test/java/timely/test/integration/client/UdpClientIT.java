package timely.test.integration.client;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.TestServer;
import timely.api.request.MetricRequest;
import timely.auth.AuthCache;
import timely.client.udp.UdpClient;
import timely.model.Metric;
import timely.model.Tag;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import timely.test.integration.InMemoryITBase;

@Category(IntegrationTest.class)
public class UdpClientIT extends InMemoryITBase {

    private static final Logger LOG = LoggerFactory.getLogger(UdpClientIT.class);
    private static final Long TEST_TIME = (System.currentTimeMillis() / 1000) * 1000;

    @Before
    public void setup() throws Exception {
        accumuloClient.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    accumuloClient.tableOperations().delete(t);
                } catch (Exception e) {}
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        AuthCache.resetConfiguration();
    }

    @Test
    public void testPut() throws Exception {
        final TestServer m = new TestServer(conf, accumuloClient);
        m.run();
        try (UdpClient client = new UdpClient("127.0.0.1", 54325)) {
            client.open();
            client.write(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n"));
            while (1 != m.getUdpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(1, m.getUdpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, m.getUdpRequests().getResponses().get(0).getClass());
            final MetricRequest actual = (MetricRequest) m.getUdpRequests().getResponses().get(0);
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
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPutMultiple() throws Exception {

        final TestServer m = new TestServer(conf, accumuloClient);
        m.run();
        try (UdpClient client = new UdpClient("127.0.0.1", 54325)) {
            client.open();
            client.write(("put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2\n" + "put sys.cpu.idle " + (TEST_TIME + 1)
                            + " 1.0 tag3=value3 tag4=value4\n"));
            while (2 != m.getUdpRequests().getCount()) {
                Thread.sleep(5);
            }
            Assert.assertEquals(2, m.getUdpRequests().getResponses().size());
            Assert.assertEquals(MetricRequest.class, m.getUdpRequests().getResponses().get(0).getClass());
            MetricRequest actual = (MetricRequest) m.getUdpRequests().getResponses().get(0);
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

            Assert.assertEquals(MetricRequest.class, m.getUdpRequests().getResponses().get(1).getClass());
            actual = (MetricRequest) m.getUdpRequests().getResponses().get(1);
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
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPutInvalidTimestamp() throws Exception {
        final TestServer m = new TestServer(conf, accumuloClient);
        m.run();
        try (UdpClient client = new UdpClient("127.0.0.1", 54325)) {
            client.open();
            client.write(("put sys.cpu.user " + TEST_TIME + "Z" + " 1.0 tag1=value1 tag2=value2\n"));
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            Assert.assertEquals(0, m.getUdpRequests().getCount());
        } finally {
            m.shutdown();
        }
    }

    @Test
    public void testPersistence() throws Exception {
        startServer();
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                            "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            assertTrue(accumuloClient.namespaceOperations().exists("timely"));
            assertTrue(accumuloClient.tableOperations().exists("timely.metrics"));
            assertTrue(accumuloClient.tableOperations().exists("timely.meta"));
            int count = 0;
            for (final Entry<Key,Value> entry : accumuloClient.createScanner("timely.metrics", Authorizations.EMPTY)) {
                final double value = ByteBuffer.wrap(entry.getValue().get()).getDouble();
                assertEquals(1.0, value, 1e-9);
                count++;
            }
            assertEquals(6, count);
            count = 0;
            for (final Entry<Key,Value> entry : accumuloClient.createScanner("timely.meta", Authorizations.EMPTY)) {
                count++;
            }
            assertEquals(10, count);
            // count w/out versioning iterator to make sure that the optimization
            // for writing is working
            accumuloClient.tableOperations().removeIterator("timely.meta", "vers", EnumSet.of(IteratorScope.scan));
            // wait for zookeeper propagation
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);
            count = 0;
            for (final Entry<Key,Value> entry : accumuloClient.createScanner("timely.meta", Authorizations.EMPTY)) {
                count++;
            }
            assertEquals(15, count);
        } finally {
            stopServer();
        }
    }

    @Test
    public void testPersistenceWithVisibility() throws Exception {
        startServer();
        try {
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
        } finally {
            stopServer();
        }
    }

    private void put(String... lines) throws Exception {
        StringBuffer format = new StringBuffer();
        for (String line : lines) {
            format.append("put ");
            format.append(line);
            format.append("\n");
        }
        try (UdpClient client = new UdpClient("127.0.0.1", 54325)) {
            client.open();
            client.write(format.toString());
        }
    }

}

package timely.collectd.plugin;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;

import net.jcip.annotations.NotThreadSafe;

import org.collectd.api.Collectd;
import org.collectd.api.DataSet;
import org.collectd.api.DataSource;
import org.collectd.api.OConfigItem;
import org.collectd.api.ValueList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Collectd.class })
@NotThreadSafe
@PowerMockIgnore("javax.management.*")
public class WriteTimelyPluginTest {

    private WriteTimelyPlugin plugin = null;
    private FakeServer server = null;

    @Before
    public void setup() throws Exception {
        PowerMock.suppress(PowerMock.everythingDeclaredIn(Collectd.class));
        server = new FakeServer();
    }

    private ValueList createMetric() {
        ValueList val = new ValueList();
        val.setHost("r001n01.localdomain");
        val.setTime(System.currentTimeMillis());
        val.setPlugin("hddtemp");
        val.setPluginInstance(null);
        val.setType("temperature");
        val.setTypeInstance("sda");
        val.setValues(Collections.singletonList((Number) new Double(2.0)));
        DataSet ds = new DataSet("test");
        ds.addDataSource(new DataSource("test", DataSource.TYPE_COUNTER, 0, 100));
        val.setDataSet(ds);
        return val;
    }

    private void setupPlugin() throws Exception {
        OConfigItem host = new OConfigItem("Host");
        host.addValue(server.getHost());
        OConfigItem port = new OConfigItem("Port");
        port.addValue(Integer.toString(server.getPort()));
        OConfigItem config = new OConfigItem("");
        config.addChild(host);
        config.addChild(port);
        plugin = new WriteTimelyPlugin();
        Assert.assertEquals(0, plugin.config(config));
        // Seed with one metric to create the connection
        Assert.assertEquals(0, plugin.write(createMetric()));
        Thread.sleep(100);
        Assert.assertTrue(server.messageReceived());
    }

    @Test
    public void testWrite() throws Exception {
        Thread t = new Thread(server);
        t.start();
        setupPlugin();
        while (!server.ready()) {
            Thread.sleep(1000);
        }
        Assert.assertEquals(0, plugin.write(createMetric()));
        Thread.sleep(100);
        Assert.assertTrue(server.messageReceived());
        plugin.shutdown();
        server.shutdown();
        t.join();
    }

    @Test
    public void testWriteAfterServerRestart() throws Exception {
        Thread t = new Thread(server);
        t.start();
        setupPlugin();
        while (!server.ready()) {
            Thread.sleep(1000);
        }
        Assert.assertEquals(0, plugin.write(createMetric()));
        Thread.sleep(100);
        Assert.assertTrue(server.messageReceived());
        server.shutdown();
        t.join();
        Thread.sleep(2000);

        server.create();
        Thread t2 = new Thread(server);
        t2.start();
        // Need to call this again because the server is not guaranteed to be
        // listening on the same local port as the first time that it was
        // started
        setupPlugin();
        while (!server.ready()) {
            Thread.sleep(1000);
            // Keep sending metrics to plugin to force reconnect
            int result = plugin.write(createMetric());
            System.out.println("Wrote to client, result: " + result);
            Assert.assertEquals(0, result);
        }
        Assert.assertEquals(0, plugin.write(createMetric()));
        Thread.sleep(1000);
        Assert.assertTrue(server.messageReceived());
        plugin.shutdown();
        server.shutdown();
        t2.join();
    }

    private static class FakeServer implements Runnable {

        private ServerSocket server = null;
        private BufferedReader reader = null;
        private String host = null;
        private int port = 0;
        private Socket s = null;
        private boolean received = false;

        public FakeServer() throws Exception {
            create();
        }

        public void create() throws Exception {
            server = new ServerSocket(port);
            server.setReceiveBufferSize(8192);
            host = server.getInetAddress().getHostAddress();
            port = server.getLocalPort();
            System.out.println("Local port " + port);
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public void shutdown() throws Exception {
            System.out.println("Closing server.");
            s.close();
            s = null;
            reader.close();
            reader = null;
            server.close();
            server = null;
        }

        public boolean ready() throws Exception {
            return (s != null && s.isConnected());
        }

        public boolean messageReceived() {
            try {
                return Boolean.valueOf(this.received);
            } finally {
                this.received = false;
            }
        }

        @Override
        public void run() {
            try {
                s = server.accept();
                System.out.println("Client connected.");
                reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
                while (server != null && !server.isClosed() && s != null && s.isConnected()) {
                    String line;
                    try {
                        line = reader.readLine();
                    } catch (Exception e) {
                        // Might have been closed
                        System.out.println("Closed by Exception: " + e.getMessage());
                        return;
                    }
                    if (null != line) {
                        this.received = true;
                    }
                    System.out.println(line);
                }
            } catch (IOException e) {
                Assert.fail(e.getMessage());
            }
            System.out.println("Exiting run method.");
        }

    }

}

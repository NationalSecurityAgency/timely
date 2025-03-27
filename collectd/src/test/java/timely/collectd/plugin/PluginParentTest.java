package timely.collectd.plugin;

import static org.junit.Assert.*;

import java.io.OutputStream;
import java.net.URL;
import java.util.Collections;

import org.collectd.api.Collectd;
import org.collectd.api.DataSet;
import org.collectd.api.DataSource;
import org.collectd.api.ValueList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({Collectd.class})
public class PluginParentTest {

    public class TestPlugin extends CollectDPluginParent {

        public TestPlugin() {
            super.addlTags.put("addl1", "foo");
        }

        @Override
        public void write(String metric, OutputStream out) {
            result = metric;
        }

    }

    private static final String HOST = "r01n01.test";
    private static final Long TIME = 1456156976840L;
    public String result = null;

    @Before
    public void setup() {
        result = null;
        PowerMock.suppress(PowerMock.everythingDeclaredIn(Collectd.class));
    }

    @Test
    public void testStatsDHadoopFormat() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("statsd");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("DataNode.dfs.datanode.BlocksRead");
        vl.setValues(Collections.singletonList((Number) 1.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put statsd.dfs.BlocksRead 1456156976840 1.0 addl1=foo host=r01n01 instance=DataNode rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testStatsDUnknownFormat() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("statsd");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("baz");
        vl.setValues(Collections.singletonList((Number) 1.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put statsd.baz 1456156976840 1.0 addl1=foo host=r01n01 rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testStatsDUnknownFormat2() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("statsd");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("bar.baz");
        vl.setValues(Collections.singletonList((Number) 1.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put statsd.baz 1456156976840 1.0 addl1=foo host=r01n01 instance=bar rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testHddTemp() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("hddtemp");
        vl.setTime(TIME);
        vl.setType("temperature");
        vl.setTypeInstance("sda");
        vl.setValues(Collections.singletonList((Number) 35.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.hddtemp.temperature 1456156976840 35.0 addl1=foo host=r01n01 instance=sda rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testSmart1() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("smart");
        vl.setPluginInstance("sda");
        vl.setTime(TIME);
        vl.setType("smart_badsectors");
        vl.setValues(Collections.singletonList((Number) 0.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.smart.smart_badsectors 1456156976840 0.0 addl1=foo host=r01n01 instance=sda rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testSmart2() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("smart");
        vl.setPluginInstance("sda");
        vl.setTime(TIME);
        vl.setType("smart_attribute");
        vl.setTypeInstance("raw-read-error-rate");
        vl.setValues(Collections.singletonList((Number) 0.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.smart.raw-read-error-rate 1456156976840 0.0 addl1=foo host=r01n01 instance=sda rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testSmartCode() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("smart");
        vl.setPluginInstance("sda");
        vl.setTime(TIME);
        vl.setType("smart_attribute");
        vl.setTypeInstance("attribute-242");
        vl.setValues(Collections.singletonList((Number) 0.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.smart.Total_LBAs_Read 1456156976840 0.0 addl1=foo code=242 host=r01n01 instance=sda rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testSnmp() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("snmp");
        // vl.setPluginInstance("sda");
        vl.setTime(TIME);
        vl.setType("if_octets");
        vl.setTypeInstance("Ethernet1");
        vl.setValues(Collections.singletonList((Number) 0.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.snmp.if_octets 1456156976840 0.0 addl1=foo host=r01n01 instance=Ethernet1 rack=r01 sampleType=GAUGE\n", result);

    }

    @Test
    public void testSensors() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("sensors");
        vl.setPluginInstance("coretemp-isa-0000");
        vl.setTime(TIME);
        vl.setType("temperature");
        vl.setTypeInstance("temp1");
        vl.setValues(Collections.singletonList((Number) 35.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.sensors.temperature.coretemp-isa-0000 1456156976840 35.0 addl1=foo host=r01n01 instance=1 rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testHAProxy1() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("haproxy");
        vl.setTime(TIME);
        vl.setType("gauge");
        vl.setTypeInstance("run_queue");
        vl.setValues(Collections.singletonList((Number) 0.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.haproxy.run_queue 1456156976840 0.0 addl1=foo host=r01n01 rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testHAProxy2() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("haproxy");
        vl.setPluginInstance("[proxy_name=proxy1,service_name=server1]");
        vl.setTime(TIME);
        vl.setType("gauge");
        vl.setTypeInstance("queue_current");
        vl.setValues(Collections.singletonList((Number) 0.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.haproxy.queue_current 1456156976840 0.0 addl1=foo host=r01n01 proxy_name=proxy1 rack=r01 sampleType=GAUGE service_name=server1\n",
                        result);
    }

    @Test
    public void testEthStatNoQueue() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("ethstat");
        vl.setPluginInstance("eth0");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("tx_comp_queue_full");
        vl.setValues(Collections.singletonList((Number) 6.0D));
        DataSet ds = new DataSet("DERIVE");
        ds.addDataSource(new DataSource("value", 1, 6.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.ethstat.tx_comp_queue_full 1456156976840 6.0 addl1=foo host=r01n01 instance=eth0 rack=r01 sampleType=GAUGE\n", result);

    }

    @Test
    public void testEthstatWithQueueStyle1() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("ethstat");
        vl.setPluginInstance("eth0");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("rx_queue_15_bytes");
        vl.setValues(Collections.singletonList((Number) 6.0D));
        DataSet ds = new DataSet("DERIVE");
        ds.addDataSource(new DataSource("value", 1, 6.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.ethstat.rx_queue_bytes 1456156976840 6.0 addl1=foo host=r01n01 instance=eth0 queue=15 rack=r01 sampleType=GAUGE\n", result);

        result = null;
        vl.setTypeInstance("queue_7_tx_bytes");
        test.process(vl, null);
        assertEquals("put sys.ethstat.queue_tx_bytes 1456156976840 6.0 addl1=foo host=r01n01 instance=eth0 queue=7 rack=r01 sampleType=GAUGE\n", result);

        result = null;
        vl.setTypeInstance("queue_7_rx_xdp_drop");
        test.process(vl, null);
        assertEquals("put sys.ethstat.queue_rx_xdp_drop 1456156976840 6.0 addl1=foo host=r01n01 instance=eth0 queue=7 rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testEthstatWithQueueStyle2() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("ethstat");
        vl.setPluginInstance("eth0");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("rx-15.bytes");
        vl.setValues(Collections.singletonList((Number) 6.0D));
        DataSet ds = new DataSet("DERIVE");
        ds.addDataSource(new DataSource("value", 1, 6.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();

        test.process(vl, null);
        assertEquals("put sys.ethstat.rx_queue_bytes 1456156976840 6.0 addl1=foo host=r01n01 instance=eth0 queue=15 rack=r01 sampleType=GAUGE\n", result);

        result = null;
        vl.setTypeInstance("tx15_xdp_err");
        test.process(vl, null);
        assertEquals("put sys.ethstat.tx_queue_xdp_err 1456156976840 6.0 addl1=foo host=r01n01 instance=eth0 queue=15 rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testEthstatWithTrafficClass() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("ethstat");
        vl.setPluginInstance("eth0");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("veb.tc_15_tx_bytes");
        vl.setValues(Collections.singletonList((Number) 6.0D));
        DataSet ds = new DataSet("DERIVE");
        ds.addDataSource(new DataSource("value", 1, 6.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();

        test.process(vl, null);
        assertEquals("put sys.ethstat.veb.tc_tx_bytes 1456156976840 6.0 addl1=foo host=r01n01 instance=eth0 rack=r01 sampleType=GAUGE trafficClass=15\n",
                        result);
    }

    @Test
    public void testEthstatWithChannel() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("ethstat");
        vl.setPluginInstance("eth0");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("ch44_events");
        vl.setValues(Collections.singletonList((Number) 6.0D));
        DataSet ds = new DataSet("DERIVE");
        ds.addDataSource(new DataSource("value", 1, 6.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();

        test.process(vl, null);
        assertEquals("put sys.ethstat.ch_events 1456156976840 6.0 addl1=foo channel=44 host=r01n01 instance=eth0 rack=r01 sampleType=GAUGE\n", result);
    }

    @Test
    public void testIpmi() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("ipmi");
        vl.setTime(TIME);
        vl.setType("temperature");
        vl.setTypeInstance("LAN NIC Temp system_board (3.2)");
        vl.setValues(Collections.singletonList((Number) 6.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 6.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl, null);
        assertEquals("put sys.ipmi.temperature 1456156976840 6.0 addl1=foo host=r01n01 instance=LAN_NIC_Temp_system_board_(3.2) rack=r01 sampleType=GAUGE\n",
                        result);

    }

    @Test
    public void testExclusions() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("ethstat");
        vl.setPluginInstance("eth0");
        vl.setTime(TIME);
        vl.setType("derive");
        vl.setTypeInstance("rx_queue_1_bytes");
        vl.setValues(Collections.singletonList((Number) 6.0D));
        DataSet ds = new DataSet("DERIVE");
        ds.addDataSource(new DataSource("value", 1, 6.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        URL filteredMetricsFile = getClass().getClassLoader().getResource("filteredMetrics.txt");
        test.setFilteredMetricsFile(filteredMetricsFile.getPath());
        URL filteredTagsFile = getClass().getClassLoader().getResource("filteredTags.txt");
        test.setFilteredTagsFile(filteredTagsFile.getPath());

        test.process(vl, null);
        assertEquals("put sys.ethstat.rx_queue_bytes 1456156976840 6.0 addl1=foo host=r01n01 queue=1 rack=r01\n", result);

        result = null;
        vl.setPlugin("interface");
        vl.setTypeInstance("if_octets");
        assertNull(result);
    }
}

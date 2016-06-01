package timely.collectd.plugin;

import static org.junit.Assert.*;

import java.util.Collections;

import net.jcip.annotations.NotThreadSafe;

import org.collectd.api.Collectd;
import org.collectd.api.DataSet;
import org.collectd.api.DataSource;
import org.collectd.api.ValueList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Collectd.class })
@NotThreadSafe
public class PluginParentTest {

    public class TestPlugin extends CollectDPluginParent {

        public TestPlugin() {
            super.addlTags.add(" addl1=foo");
        }

        @Override
        public void write(String metric) {
            result = metric;
        }

    }

    private static final String HOST = "r01n01.test";
    private static final Long TIME = 1456156976840L;
    public String result = null;

    @Before
    public void setup() throws Exception {
        PowerMock.suppress(PowerMock.everythingDeclaredIn(Collectd.class));
    }

    @Test
    public void testStatsDHadoopFormat() throws Exception {
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
        test.process(vl);
        assertEquals(
                "put statsd.dfs.BlocksRead 1456156976840 1.0 host=r01n01 rack=r01 addl1=foo instance=DataNode sample=value sampleType=GAUGE\n",
                result);
    }

    @Test
    public void testStatsDUnknownFormat() throws Exception {
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
        test.process(vl);
        assertEquals("put statsd.baz 1456156976840 1.0 host=r01n01 rack=r01 addl1=foo sample=value sampleType=GAUGE\n",
                result);
    }

    @Test
    public void testStatsDUnknownFormat2() throws Exception {
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
        test.process(vl);
        assertEquals(
                "put statsd.baz 1456156976840 1.0 host=r01n01 rack=r01 addl1=foo instance=bar sample=value sampleType=GAUGE\n",
                result);
    }

    @Test
    public void testHddTemp() throws Exception {
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
        test.process(vl);
        assertEquals(
                "put sys.hddtemp.temperature 1456156976840 35.0 host=r01n01 rack=r01 addl1=foo instance=sda sample=value sampleType=GAUGE\n",
                result);
    }

    @Test
    public void testSmart1() throws Exception {
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
        test.process(vl);
        assertEquals(
                "put sys.smart.smart_badsectors 1456156976840 0.0 host=r01n01 rack=r01 addl1=foo instance=sda sample=value sampleType=GAUGE\n",
                result);
    }

    @Test
    public void testSmart2() throws Exception {
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
        test.process(vl);
        assertEquals(
                "put sys.smart.raw-read-error-rate 1456156976840 0.0 host=r01n01 rack=r01 addl1=foo instance=sda sample=value sampleType=GAUGE\n",
                result);
    }

    @Test
    public void testSmartCode() throws Exception {
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
        test.process(vl);
        assertEquals(
                "put sys.smart.Total_LBAs_Read 1456156976840 0.0 host=r01n01 rack=r01 addl1=foo code=242 instance=sda sample=value sampleType=GAUGE\n",
                result);
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
        test.process(vl);
        assertEquals(
                "put sys.sensors.temperature.coretemp-isa-0000 1456156976840 35.0 host=r01n01 rack=r01 addl1=foo instance=1 sample=value sampleType=GAUGE\n",
                result);
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
        test.process(vl);
        assertEquals(
                "put sys.haproxy.run_queue 1456156976840 0.0 host=r01n01 rack=r01 addl1=foo sample=value sampleType=GAUGE\n",
                result);
    }

    @Test
    public void testHAProxy2() {
        ValueList vl = new ValueList();
        vl.setHost(HOST);
        vl.setPlugin("haproxy");
        vl.setTime(TIME);
        vl.setType("gauge");
        vl.setTypeInstance("server1.proxy1.queue_current");
        vl.setValues(Collections.singletonList((Number) 0.0D));
        DataSet ds = new DataSet("GAUGE");
        ds.addDataSource(new DataSource("value", 1, 0.0D, 100.0D));
        vl.setDataSet(ds);

        TestPlugin test = new TestPlugin();
        test.process(vl);
        assertEquals(
                "put sys.haproxy.queue_current 1456156976840 0.0 host=r01n01 rack=r01 addl1=foo proxy=proxy1 server=server1 sample=value sampleType=GAUGE\n",
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
        test.process(vl);
        assertEquals(
                "put sys.ethstat.tx_comp_queue_full 1456156976840 6.0 host=r01n01 rack=r01 addl1=foo instance=eth0 sample=value sampleType=GAUGE\n",
                result);

    }

    @Test
    public void testEthStatWithQueue() {
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
        test.process(vl);
        assertEquals(
                "put sys.ethstat.rx_bytes 1456156976840 6.0 host=r01n01 rack=r01 addl1=foo queue=1 instance=eth0 sample=value sampleType=GAUGE\n",
                result);

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
        test.process(vl);
        assertEquals(
                "put sys.ipmi.temperature.LAN_NIC_Temp_system_board_(3.2) 1456156976840 6.0 host=r01n01 rack=r01 addl1=foo sample=value sampleType=GAUGE\n",
                result);

    }

}

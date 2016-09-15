package timely.netty.udp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import timely.Configuration;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.MetricRequest;
import timely.model.Metric;
import timely.model.Tag;
import timely.auth.VisibilityCache;

public class UdpDecoderTest {

    private static final Long TEST_TIME = System.currentTimeMillis();

    @Before
    public void setup() {
        VisibilityCache.init(new Configuration());
    }

    @Test
    public void testPutNoViz() throws Exception {
        UdpDecoder decoder = new UdpDecoder();
        List<Object> results = new ArrayList<Object>();
        String put = "put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2";
        ByteBuf buf = Unpooled.wrappedBuffer(put.getBytes());
        decoder.decode(null, buf, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(MetricRequest.class, results.get(0).getClass());
        Metric m = ((MetricRequest) results.get(0)).getMetric();
        Metric expected = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D)
                .tag(new Tag("tag1", "value1")).tag(new Tag("tag2", "value2")).build();
        Assert.assertEquals(expected, m);
    }

    @Test
    public void testPutWithViz() throws Exception {
        UdpDecoder decoder = new UdpDecoder();
        List<Object> results = new ArrayList<Object>();
        String put = "put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 viz=a&b tag2=value2";
        ByteBuf buf = Unpooled.wrappedBuffer(put.getBytes());
        decoder.decode(null, buf, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(MetricRequest.class, results.get(0).getClass());
        Metric m = ((MetricRequest) results.get(0)).getMetric();
        Metric expected = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D)
                .tag(new Tag("tag1", "value1")).tag(new Tag("tag2", "value2")).tag(MetricAdapter.VISIBILITY_TAG, "a&b")
                .build();
        Assert.assertEquals(expected, m);
    }

    @Test
    public void testUnknownOperation() throws Exception {
        UdpDecoder decoder = new UdpDecoder();
        List<Object> results = new ArrayList<Object>();
        String put = "version";
        ByteBuf buf = Unpooled.wrappedBuffer(put.getBytes());
        decoder.decode(null, buf, results);
        Assert.assertEquals(0, results.size());
    }

}

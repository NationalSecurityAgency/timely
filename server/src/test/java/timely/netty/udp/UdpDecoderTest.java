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
import timely.api.model.Metric;
import timely.api.model.Tag;
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
        Assert.assertEquals(Metric.class, results.get(0).getClass());
        Metric m = (Metric) results.get(0);
        Metric expected = new Metric();
        expected.setMetric("sys.cpu.user");
        expected.setTimestamp(TEST_TIME);
        expected.setValue(1.0D);
        final List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        tags.add(new Tag("tag2", "value2"));
        expected.setTags(tags);
        expected.setVisibility(Metric.EMPTY_VISIBILITY);
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
        Assert.assertEquals(Metric.class, results.get(0).getClass());
        Metric m = (Metric) results.get(0);
        Metric expected = new Metric();
        expected.setMetric("sys.cpu.user");
        expected.setTimestamp(TEST_TIME);
        expected.setValue(1.0D);
        final List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        tags.add(new Tag("tag2", "value2"));
        expected.setTags(tags);
        ColumnVisibility cv = VisibilityCache.getColumnVisibility("a&b");
        expected.setVisibility(cv);
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

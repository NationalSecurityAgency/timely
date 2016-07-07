package timely.netty.tcp;

import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import timely.api.model.Metric;
import timely.api.model.Tag;
import timely.api.request.Version;

public class TcpDecoderTest {

    private static final Long TEST_TIME = System.currentTimeMillis();

    @Test
    public void testTcpVersion() throws Exception {
        TcpDecoder decoder = new TcpDecoder();
        List<Object> out = new ArrayList<>();
        decoder.decode(null, Unpooled.wrappedBuffer("version".getBytes()), out);
        Assert.assertEquals(1, out.size());
        Assert.assertTrue(out.get(0) instanceof Version);
        Version v = (Version) out.get(0);
        Assert.assertEquals("0.0.2", v.getVersion());
    }

    @Test
    public void testPutMetric() throws Exception {
        TcpDecoder decoder = new TcpDecoder();
        List<Object> out = new ArrayList<>();
        String metric = "put sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2";
        decoder.decode(null, Unpooled.wrappedBuffer(metric.getBytes()), out);
        Assert.assertEquals(1, out.size());
        Assert.assertTrue(out.get(0) instanceof Metric);
        Metric actual = (Metric) out.get(0);
        final Metric expected = new Metric();
        expected.setMetric("sys.cpu.user");
        expected.setTimestamp(TEST_TIME);
        expected.setValue(1.0);
        final List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        tags.add(new Tag("tag2", "value2"));
        expected.setTags(tags);
        Assert.assertEquals(expected, actual);
    }

}

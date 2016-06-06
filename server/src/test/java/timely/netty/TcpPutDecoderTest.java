package timely.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import timely.api.model.Metric;
import timely.api.model.Tag;
import timely.auth.VisibilityCache;
import timely.netty.tcp.TcpPutDecoder;

public class TcpPutDecoderTest {

    private static class TestTcpPutDecoder extends TcpPutDecoder {

        @Override
        public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            super.decode(ctx, in, out);
        }

    }

    private TestTcpPutDecoder decoder = new TestTcpPutDecoder();
    private static final long TEST_TIME = System.currentTimeMillis();

    @Before
    public void setup() {
        VisibilityCache.init();
    }

    @Test
    public void testParseNoViz() throws Exception {
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
    public void testParseWithViz() throws Exception {
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

}

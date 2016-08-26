package timely.netty.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Test;

import timely.api.model.Metric;
import timely.api.model.Tag;

import com.google.flatbuffers.FlatBufferBuilder;

public class MetricsBufferDecoderTest {

    private static final Long TEST_TIME = System.currentTimeMillis();

    private int createMetric(FlatBufferBuilder builder, String name, long timestamp, double value,
            Map<String, String> tags) {
        int n = builder.createString(name);
        int[] t = new int[tags.size()];
        int i = 0;
        for (Entry<String, String> e : tags.entrySet()) {
            t[i] = timely.api.flatbuffer.Tag.createTag(builder, builder.createString(e.getKey()),
                    builder.createString(e.getValue()));
            i++;
        }
        return timely.api.flatbuffer.Metric.createMetric(builder, n, timestamp, value,
                timely.api.flatbuffer.Metric.createTagsVector(builder, t));
    }

    @Test
    public void testBuffer() throws Exception {
        FlatBufferBuilder builder = new FlatBufferBuilder(1);

        int[] metric = new int[1];
        Map<String, String> t = new HashMap<>();
        t.put("tag1", "value1");
        t.put("tag2", "value2");
        metric[0] = createMetric(builder, "sys.cpu.user", TEST_TIME, 1.0D, t);

        int metricVector = timely.api.flatbuffer.Metrics.createMetricsVector(builder, metric);

        timely.api.flatbuffer.Metrics.startMetrics(builder);
        timely.api.flatbuffer.Metrics.addMetrics(builder, metricVector);
        int metrics = timely.api.flatbuffer.Metrics.endMetrics(builder);
        timely.api.flatbuffer.Metrics.finishMetricsBuffer(builder, metrics);

        ByteBuf buf = Unpooled.wrappedBuffer(builder.dataBuffer());
        MetricsBufferDecoder decoder = new MetricsBufferDecoder();
        List<Object> results = new ArrayList<Object>();
        decoder.decode(null, buf, results);

        Assert.assertEquals(1, results.size());
        Metric expected = new Metric();
        expected.setMetric("sys.cpu.user");
        expected.setTimestamp(TEST_TIME);
        expected.setValue(1.0D);
        final List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        tags.add(new Tag("tag2", "value2"));
        expected.setTags(tags);
        Assert.assertEquals(expected, results.get(0));
    }

    @Test
    public void testPutMultipleBuffers() throws Exception {

        FlatBufferBuilder builder = new FlatBufferBuilder(1);

        int[] metric = new int[2];
        Map<String, String> t = new HashMap<>();
        t.put("tag1", "value1");
        t.put("tag2", "value2");
        metric[0] = createMetric(builder, "sys.cpu.user", TEST_TIME, 1.0D, t);
        t = new HashMap<>();
        t.put("tag3", "value3");
        t.put("tag4", "value4");
        metric[1] = createMetric(builder, "sys.cpu.idle", TEST_TIME, 1.0D, t);

        int metricVector = timely.api.flatbuffer.Metrics.createMetricsVector(builder, metric);

        timely.api.flatbuffer.Metrics.startMetrics(builder);
        timely.api.flatbuffer.Metrics.addMetrics(builder, metricVector);
        int metrics = timely.api.flatbuffer.Metrics.endMetrics(builder);
        timely.api.flatbuffer.Metrics.finishMetricsBuffer(builder, metrics);

        ByteBuf buf = Unpooled.wrappedBuffer(builder.dataBuffer());
        MetricsBufferDecoder decoder = new MetricsBufferDecoder();
        List<Object> results = new ArrayList<Object>();
        decoder.decode(null, buf, results);

        Assert.assertEquals(2, results.size());
        Metric expected = new Metric();
        expected.setMetric("sys.cpu.user");
        expected.setTimestamp(TEST_TIME);
        expected.setValue(1.0D);
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        tags.add(new Tag("tag2", "value2"));
        expected.setTags(tags);
        Assert.assertEquals(expected, results.get(0));
        expected = new Metric();
        expected.setMetric("sys.cpu.idle");
        expected.setTimestamp(TEST_TIME);
        expected.setValue(1.0D);
        tags = new ArrayList<>();
        tags.add(new Tag("tag3", "value3"));
        tags.add(new Tag("tag4", "value4"));
        expected.setTags(tags);
        Assert.assertEquals(expected, results.get(1));
    }

}

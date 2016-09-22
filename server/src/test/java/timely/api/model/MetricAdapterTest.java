package timely.api.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.util.ComparablePair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import timely.Configuration;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.response.MetricResponse;
import timely.auth.VisibilityCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.util.JsonUtil;

public class MetricAdapterTest {

    @Before
    public void before() {
        VisibilityCache.init(new Configuration());
    }

    @Test
    public void testToMutation() throws Exception {
        long ts = System.currentTimeMillis();
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        tags.add(new Tag("tag2", "value2"));
        tags.add(new Tag("tag3", "value3"));
        Metric m = Metric.newBuilder().name("sys.cpu.user").value(ts, 2.0D).tags(tags).build();
        Mutation mut = MetricAdapter.toMutation(m);

        PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(), new LongLexicoder());
        byte[] row = rowCoder.encode(new ComparablePair<>("sys.cpu.user", ts));
        byte[] value = new byte[Double.BYTES];
        ByteBuffer.wrap(value).putDouble(2.0D);
        Assert.assertEquals(rowCoder.decode(row), rowCoder.decode(mut.getRow()));
        Assert.assertEquals(3, mut.getUpdates().size());
        ColumnUpdate up = mut.getUpdates().get(0);
        Assert.assertTrue(new String(up.getColumnFamily()).equals("tag1=value1"));
        Assert.assertTrue(new String(up.getColumnQualifier()).equals("tag2=value2,tag3=value3"));
        Assert.assertEquals(ts, up.getTimestamp());
        Assert.assertTrue(new String(up.getColumnVisibility()).equals(""));
        Assert.assertArrayEquals(value, up.getValue());
        ColumnUpdate up2 = mut.getUpdates().get(1);
        Assert.assertTrue(new String(up2.getColumnFamily()).equals("tag2=value2"));
        Assert.assertTrue(new String(up2.getColumnQualifier()).equals("tag1=value1,tag3=value3"));
        Assert.assertEquals(ts, up2.getTimestamp());
        Assert.assertTrue(new String(up2.getColumnVisibility()).equals(""));
        Assert.assertArrayEquals(value, up.getValue());
        ColumnUpdate up3 = mut.getUpdates().get(2);
        Assert.assertTrue(new String(up3.getColumnFamily()).equals("tag3=value3"));
        Assert.assertTrue(new String(up3.getColumnQualifier()).equals("tag1=value1,tag2=value2"));
        Assert.assertEquals(ts, up3.getTimestamp());
        Assert.assertTrue(new String(up3.getColumnVisibility()).equals(""));
        Assert.assertArrayEquals(value, up.getValue());
    }

    @Test
    public void testToMutationWithViz() throws Exception {
        long ts = System.currentTimeMillis();
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        Metric m = Metric.newBuilder().name("sys.cpu.user").value(ts, 2.0D).tags(tags)
                .tag(MetricAdapter.VISIBILITY_TAG, "(a&b)|(c&d)").build();

        Mutation mut = MetricAdapter.toMutation(m);

        PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(), new LongLexicoder());
        byte[] row = rowCoder.encode(new ComparablePair<>("sys.cpu.user", ts));
        byte[] value = new byte[Double.BYTES];
        ByteBuffer.wrap(value).putDouble(2.0D);
        Assert.assertEquals(rowCoder.decode(row), rowCoder.decode(mut.getRow()));
        Assert.assertEquals(1, mut.getUpdates().size());
        ColumnUpdate up = mut.getUpdates().get(0);
        Assert.assertEquals("tag1=value1", new String(up.getColumnFamily()));
        Assert.assertEquals("", new String(up.getColumnQualifier()));
        Assert.assertEquals(ts, up.getTimestamp());
        Assert.assertEquals("(a&b)|(c&d)", new String(up.getColumnVisibility()));
        Assert.assertArrayEquals(value, up.getValue());
    }

    @Test
    public void testToMetricResponse() throws Exception {
        String subscriptionId = "12345";
        long ts = 1000L;
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        Metric m = Metric.newBuilder().name("sys.cpu.user").value(ts, 2.0D).tags(tags)
                .tag(MetricAdapter.VISIBILITY_TAG, "(a&b)|(c&d)").build();
        String json = JsonUtil.getObjectMapper().writeValueAsString(MetricResponse.fromMetric(m, subscriptionId));
        String expected = "{\"metric\":\"sys.cpu.user\",\"timestamp\":1000,\"value\":2.0,\"tags\":[{\"tag1\":\"value1\"},{\"viz\":\"(a&b)|(c&d)\"}],\"subscriptionId\":\"12345\"}";
        Assert.assertEquals(expected, json);
    }
}

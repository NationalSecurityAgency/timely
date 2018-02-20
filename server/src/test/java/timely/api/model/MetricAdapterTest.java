package timely.api.model;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
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
        byte[] row = rowCoder.encode(new ComparablePair<>("sys.cpu.user", MetricAdapter.roundTimestampToLastHour(ts)));
        byte[] value = new byte[Double.BYTES];
        ByteBuffer.wrap(value).putDouble(2.0D);
        Assert.assertEquals(rowCoder.decode(row), rowCoder.decode(mut.getRow()));
        Assert.assertEquals(3, mut.getUpdates().size());
        ColumnUpdate up = mut.getUpdates().get(0);
        Assert.assertEquals("tag1=value1", new String(up.getColumnFamily()));
        PairLexicoder<Long, String> colQualCoder = new PairLexicoder<>(new LongLexicoder(), new StringLexicoder());
        Assert.assertEquals(new String(colQualCoder.encode(new ComparablePair<>(ts, "tag2=value2,tag3=value3"))),
                new String(up.getColumnQualifier()));

        Assert.assertEquals(ts, up.getTimestamp());
        Assert.assertEquals("", new String(new String(up.getColumnVisibility())));
        Assert.assertArrayEquals(value, up.getValue());
        ColumnUpdate up2 = mut.getUpdates().get(1);
        Assert.assertEquals("tag2=value2", new String(up2.getColumnFamily()));
        Assert.assertEquals(new String(colQualCoder.encode(new ComparablePair<>(ts, "tag1=value1,tag3=value3"))),
                new String(up2.getColumnQualifier()));
        Assert.assertEquals(ts, up2.getTimestamp());
        Assert.assertEquals("", new String(up2.getColumnVisibility()));
        Assert.assertArrayEquals(value, up.getValue());
        ColumnUpdate up3 = mut.getUpdates().get(2);
        Assert.assertEquals("tag3=value3", new String(up3.getColumnFamily()));
        Assert.assertEquals(new String(colQualCoder.encode(new ComparablePair<>(ts, "tag1=value1,tag2=value2"))),
                new String(up3.getColumnQualifier()));
        Assert.assertEquals(ts, up3.getTimestamp());
        Assert.assertEquals("", new String(up3.getColumnVisibility()));
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
        byte[] row = rowCoder.encode(new ComparablePair<>("sys.cpu.user", MetricAdapter.roundTimestampToLastHour(ts)));
        byte[] value = new byte[Double.BYTES];
        ByteBuffer.wrap(value).putDouble(2.0D);
        Assert.assertEquals(rowCoder.decode(row), rowCoder.decode(mut.getRow()));
        Assert.assertEquals(1, mut.getUpdates().size());
        ColumnUpdate up = mut.getUpdates().get(0);
        Assert.assertEquals("tag1=value1", new String(up.getColumnFamily()));
        PairLexicoder<Long, String> colQualCoder = new PairLexicoder<>(new LongLexicoder(), new StringLexicoder());
        Assert.assertEquals(new String(colQualCoder.encode(new ComparablePair<>(ts, ""))),
                new String(up.getColumnQualifier()));
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
        String expected = "{\"metric\":\"sys.cpu.user\",\"timestamp\":1000,\"value\":2.0,\"tags\":[{\"tag1\":\"value1\"},{\"viz\":\"(a&b)|(c&d)\"}],\"subscriptionId\":\"12345\",\"complete\":false}";
        Assert.assertEquals(expected, json);
    }

    @Test
    public void testParse() throws Exception {
        PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(), new LongLexicoder());
        byte[] row = rowCoder.encode(new ComparablePair<>("sys.cpu.user", 1000L));
        byte[] value = new byte[Double.BYTES];
        ByteBuffer.wrap(value).putDouble(2.0D);
        PairLexicoder<Long, String> colQualCoder = new PairLexicoder<>(new LongLexicoder(), new StringLexicoder());
        Key k = new Key(row, "tag1=value1".getBytes(), colQualCoder.encode(new ComparablePair<>(new Long(1000),
                "tag2=value2,tag3=value3")), "(a&b)|(c&d)".getBytes(), 1000);
        Value v = new Value(value);
        Metric m = MetricAdapter.parse(k, v);
        Assert.assertEquals("sys.cpu.user", m.getName());
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1=value1"));
        tags.add(new Tag("tag2=value2"));
        tags.add(new Tag("tag3=value3"));
        Assert.assertEquals(tags, m.getTags());
        Assert.assertEquals(new Long(1000), m.getValue().getTimestamp());
        Assert.assertEquals(2.0D, m.getValue().getMeasure(), 0.0D);
    }

    @Test
    public void testParseWithViz() throws Exception {
        PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(), new LongLexicoder());
        byte[] row = rowCoder.encode(new ComparablePair<>("sys.cpu.user", 1000L));
        byte[] value = new byte[Double.BYTES];
        ByteBuffer.wrap(value).putDouble(2.0D);
        PairLexicoder<Long, String> colQualCoder = new PairLexicoder<>(new LongLexicoder(), new StringLexicoder());
        Key k = new Key(row, "tag1=value1".getBytes(), colQualCoder.encode(new ComparablePair<>(new Long(1000),
                "tag2=value2,tag3=value3")), "(a&b)|(c&d)".getBytes(), 1000);
        Value v = new Value(value);
        Metric m = MetricAdapter.parse(k, v, true);
        Assert.assertEquals("sys.cpu.user", m.getName());
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1=value1"));
        tags.add(new Tag("tag2=value2"));
        tags.add(new Tag("tag3=value3"));
        tags.add(new Tag("viz=(a&b)|(c&d)"));
        Assert.assertEquals(tags, m.getTags());
        Assert.assertEquals(new Long(1000), m.getValue().getTimestamp());
        Assert.assertEquals(2.0D, m.getValue().getMeasure(), 0.0D);
    }

}

package timely.analytics.flink;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import timely.model.Metric;
import timely.model.Tag;
import timely.model.parse.MetricParser;

public class MetricHistogramTest {

    private MetricHistogram m = null;
    private long timestamp = System.currentTimeMillis();

    @Before
    public void createMetric() throws Exception {
        m = new MetricHistogram();
        for (double d = 1.0D; d <= 100.0; d += 1.0) {
            timestamp += 1000;
            m.update(d, timestamp);
        }
        m.toString();
        Assert.assertEquals(timestamp, m.getTimestamp());
    }

    @Test
    public void testMin() throws Exception {
        Assert.assertEquals(1.0D, m.min(), 0.0D);
    }

    @Test
    public void testMax() throws Exception {
        Assert.assertEquals(100.0D, m.max(), 0.0D);
    }

    @Test
    public void testAvg() throws Exception {
        int sum = 0;
        for (int i = 1; i <= 100; i++) {
            sum += i;
        }
        Assert.assertEquals((sum / 100.0D), m.avg(), 0.0D);
    }

    @Test
    public void testCount() throws Exception {
        Assert.assertEquals(100.0D, m.count(), 0.0D);
    }

    @Test
    public void test50thPercentile() throws Exception {
        Assert.assertEquals(50.0D, m.getPercentile(50), 0.0D);
    }

    @Test
    public void test75thPercentile() throws Exception {
        Assert.assertEquals(75.0D, m.getPercentile(75), 0.0D);
    }

    @Test
    public void test90thPercentile() throws Exception {
        Assert.assertEquals(90.0D, m.getPercentile(90), 0.0D);
    }

    @Test
    public void test99thPercentile() throws Exception {
        Assert.assertEquals(99.0D, m.getPercentile(99), 0.0D);
    }

    @Test
    public void testSerialization() throws Exception {
        MetricParser metricParser = new MetricParser();
        Tag t1 = new Tag("tag1=value1");
        Tag t2 = new Tag("tag2=value2");
        Tag avg = new Tag("sample=avg");
        Tag min = new Tag("sample=min");
        Tag max = new Tag("sample=max");
        Tag sum = new Tag("sample=sum");
        Tag count = new Tag("sample=count");
        Tag p50 = new Tag("sample=50p");
        Tag p75 = new Tag("sample=75p");
        Tag p90 = new Tag("sample=90p");
        Tag p99 = new Tag("sample=99p");

        List<Tag> tags = new ArrayList<>();
        tags.add(t1);
        tags.add(t2);
        m.initialize("sys.cpu.user", tags);

        byte[] bytes = m.serialize(m);
        String puts = new String(bytes);
        for (String put : puts.split("\n")) {
            Metric metric = metricParser.parse(put);
            Assert.assertEquals("sys.cpu.user_summarized", metric.getName());
            metric.getTags().forEach(
                    t -> {
                        Assert.assertTrue(t.equals(t1) || t.equals(t2) || t.equals(avg) || t.equals(min)
                                || t.equals(max) || t.equals(sum) || t.equals(count) || t.equals(p50) || t.equals(p75)
                                || t.equals(p90) || t.equals(p99));
                    });
        }
    }

}

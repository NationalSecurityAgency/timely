package timely.sample.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Before;
import org.junit.Test;

import timely.Configuration;
import timely.adapter.accumulo.MetricAdapter;
import timely.model.Metric;
import timely.model.Tag;
import timely.auth.VisibilityCache;
import timely.sample.Downsample;
import timely.sample.Sample;
import timely.sample.aggregators.Avg;

public class DownsampleIteratorTest {

    final private SortedMap<Key, Value> testData1 = new TreeMap<>();
    final private SortedMap<Key, Value> testData2 = new TreeMap<>();

    @Before
    public void before() {
        VisibilityCache.init(new Configuration());
    }

    @Before
    public void createTestData() {
        createTestData1();
        createTestData2();
    }

    private void createTestData2() {
        List<Tag> tags = Collections.singletonList(new Tag("host", "host1"));

        for (long i = 0; i < 1000; i += 100) {
            Metric m = new Metric("sys.loadAvg", i, .2, tags);
            Mutation mutation = MetricAdapter.toMutation(m);
            for (ColumnUpdate cu : mutation.getUpdates()) {
                Key key = new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
                        cu.getColumnVisibility(), cu.getTimestamp());
                testData1.put(key, new Value(cu.getValue()));
            }
        }
    }

    void put(Map<Key, Value> testData, Metric m) {
        Mutation mutation = MetricAdapter.toMutation(m);
        for (ColumnUpdate cu : mutation.getUpdates()) {
            Key key = new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
                    cu.getColumnVisibility(), cu.getTimestamp());
            testData.put(key, new Value(cu.getValue()));
        }
    }

    private void createTestData1() {
        List<Tag> tags = Collections.singletonList(new Tag("host", "host1"));
        List<Tag> tags2 = Collections.singletonList(new Tag("host", "host2"));

        for (long i = 0; i < 1000; i += 100) {
            put(testData2, new Metric("sys.loadAvg", i, .2, tags));
            put(testData2, new Metric("sys.loadAvg", i + 50, .5, tags2));
        }
    }

    @Test
    public void simpleGetOneSample() throws Exception {
        // check that data gets pulled out
        DownsampleIterator iter = new DownsampleIterator();
        Map<Set<Tag>, Downsample> samples = runQuery(iter, testData1, 100);
        assertEquals(1, samples.size());
        for (Entry<Set<Tag>, Downsample> entry : samples.entrySet()) {
            Set<Tag> tags = entry.getKey();
            assertEquals(1, tags.size());
            assertEquals(Collections.singleton(new Tag("host", "host1")), tags);
            long ts = 0;
            for (Sample sample : entry.getValue()) {
                assertEquals(ts, sample.timestamp);
                ts += 100;
                assertEquals(0.2, sample.value, 0.0001);
            }
            assertEquals(1000, ts);
        }
    }

    @Test
    public void simpleGetTwoSamples() throws Exception {
        DownsampleIterator iter = new DownsampleIterator();
        Map<Set<Tag>, Downsample> samples = runQuery(iter, testData2, 100);
        assertEquals(2, samples.size());
        for (Tag tag : new Tag[] { new Tag("host", "host1"), new Tag("host", "host2") }) {
            Downsample dsample = samples.get(Collections.singleton(tag));
            assertNotNull(dsample);
            long ts = 0;
            double value = .2;
            if (tag.getValue().equals("host2")) {
                value = .5;
            }
            int count = 0;
            for (Sample sample : dsample) {
                assertEquals(ts, sample.timestamp);
                ts += 100;
                assertEquals(value, sample.value, 0.0001);
                count++;
            }
            assertEquals(10, count);
        }
    }

    @Test
    public void simpleTestDownsampling() throws Exception {
        DownsampleIterator iter = new DownsampleIterator();
        Map<Set<Tag>, Downsample> samples = runQuery(iter, testData2, 200);
        assertEquals(2, samples.size());
        for (Tag tag : new Tag[] { new Tag("host", "host1"), new Tag("host", "host2") }) {
            Downsample dsample = samples.get(Collections.singleton(tag));
            assertNotNull(dsample);
            long ts = 0;
            double value = .2;
            if (tag.getValue().equals("host2")) {
                value = .5;
            }
            int count = 0;
            for (Sample sample : dsample) {
                assertEquals(ts, sample.timestamp);
                ts += 200;
                assertEquals(value, sample.value, 0.0001);
                count++;
            }
            assertEquals(5, count);
        }
    }

    private Map<Set<Tag>, Downsample> runQuery(SortedKeyValueIterator<Key, Value> iter, SortedMap<Key, Value> testData,
            long period) throws Exception {
        IteratorSetting is = new IteratorSetting(100, DownsampleIterator.class);
        DownsampleIterator.setDownsampleOptions(is, 0, 1000, period, Avg.class.getName());
        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(testData);
        iter.init(source, is.getOptions(), null);
        iter.seek(new Range(), Collections.emptyList(), true);
        assertTrue(iter.hasTop());
        Key key = iter.getTopKey();
        assertEquals(testData.lastKey(), key);
        Map<Set<Tag>, Downsample> samples = DownsampleIterator.decodeValue(iter.getTopValue());
        return samples;
    }

}

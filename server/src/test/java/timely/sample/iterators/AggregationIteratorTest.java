package timely.sample.iterators;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Before;
import org.junit.Test;
import timely.adapter.accumulo.MetricAdapter;
import timely.auth.VisibilityCache;
import timely.configuration.Configuration;
import timely.model.Metric;
import timely.model.Tag;
import timely.sample.Aggregation;
import timely.sample.Downsample;
import timely.sample.Sample;
import timely.sample.aggregators.Avg;

public class AggregationIteratorTest {

    final private SortedMap<Key, Value> testData1 = new TreeMap<>();
    final private SortedMap<Key, Value> testData2 = new TreeMap<>();

    @Before
    public void before() {
        VisibilityCache.init(new Configuration());
    }

    @Before
    public void createTestData() throws Exception {
        createTestData1();
        createTestData2();
    }

    private void createTestData1() throws Exception {
        List<Tag> tags = Collections.singletonList(new Tag("host", "host1"));

        Downsample sample = new Downsample(0L, 10000L, 1L, new Avg());
        Metric m = null;
        for (long i = 0; i < 1000; i += 100) {
            m = new Metric("sys.loadAvg", i, .2, tags);
            sample.add(i, .2);
        }

        put(testData1, m, sample);
    }

    /**
     * This will add key, values to the test data as output from the
     * DownsampleIterator
     * 
     * @param testData
     * @param m
     * @param sample
     * @throws Exception
     */
    void put(Map<Key, Value> testData, Metric m, Downsample sample) throws Exception {
        Mutation mutation = MetricAdapter.toMutation(m);
        for (ColumnUpdate cu : mutation.getUpdates()) {
            Key key = new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
                    cu.getColumnVisibility(), cu.getTimestamp());
            Map<Set<Tag>, Downsample> samples = new HashMap<>();
            samples.put(new HashSet<Tag>(m.getTags()), sample);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(samples);
            out.flush();
            testData.put(key, new Value(bos.toByteArray()));
        }
    }

    private void createTestData2() throws Exception {
        List<Tag> tags = Collections.singletonList(new Tag("host", "host1"));
        List<Tag> tags2 = Collections.singletonList(new Tag("host", "host2"));

        Downsample sample = new Downsample(0L, 10000L, 1L, new Avg());
        Metric m = null;
        for (long i = 0; i < 1000; i += 100) {
            m = new Metric("sys.loadAvg", i, .2, tags);
            sample.add(i, .2);
        }
        put(testData2, m, sample);

        sample = new Downsample(0L, 10000L, 1L, new Avg());
        m = null;
        for (long i = 0; i < 1000; i += 100) {
            m = new Metric("sys.loadAvg", i + 100, .5, tags2);
            sample.add(i + 100, .5);
        }
        put(testData2, m, sample);
    }

    @Test
    public void simpleGetOneSample() throws Exception {
        // check that data gets pulled out
        AggregationIterator iter = new AggregationIterator();
        Map<Set<Tag>, Aggregation> samples = runQuery(iter, testData1, 100);
        assertEquals(1, samples.size());
        for (Entry<Set<Tag>, Aggregation> entry : samples.entrySet()) {
            Set<Tag> tags = entry.getKey();
            assertEquals(1, tags.size());
            assertEquals(Collections.singleton(new Tag("host", ".*")), tags);
            long ts = 0;
            int count = 0;
            for (Sample sample : entry.getValue()) {
                assertEquals(ts, sample.timestamp);
                ts += 100;
                assertEquals(0.2, sample.value, 0.0001);
                count++;
            }
            assertEquals(1000, ts);
            assertEquals(10, count);
        }
    }

    @Test
    public void simpleAggregatedSample() throws Exception {
        AggregationIterator iter = new AggregationIterator();
        Map<Set<Tag>, Aggregation> samples = runQuery(iter, testData2, 100);
        assertEquals(1, samples.size());
        for (Entry<Set<Tag>, Aggregation> entry : samples.entrySet()) {
            Set<Tag> tags = entry.getKey();
            assertEquals(1, tags.size());
            assertEquals(Collections.singleton(new Tag("host", ".*")), tags);
            long ts = 0;
            int count = 0;
            for (Sample sample : entry.getValue()) {
                assertEquals(ts, sample.timestamp);
                ts += 100;
                assertEquals(count == 0 ? 0.2 : (count == 10 ? 0.5 : 0.35), sample.value, 0.0001);
                count++;
            }
            assertEquals(11, count);
        }
    }

    private Map<Set<Tag>, Aggregation> runQuery(SortedKeyValueIterator<Key, Value> iter, SortedMap<Key, Value> testData,
            long period) throws Exception {
        IteratorSetting is = new IteratorSetting(100, AggregationIterator.class);
        AggregationIterator.setAggregationOptions(is, Collections.singletonMap("host", ".*"), Avg.class.getName());
        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(testData);
        iter.init(source, is.getOptions(), null);
        iter.seek(new Range(), Collections.emptyList(), true);
        assertTrue(iter.hasTop());
        Key key = iter.getTopKey();
        assertEquals(testData.lastKey(), key);
        Map<Set<Tag>, Aggregation> samples = AggregationIterator.decodeValue(iter.getTopValue());
        return samples;
    }

}

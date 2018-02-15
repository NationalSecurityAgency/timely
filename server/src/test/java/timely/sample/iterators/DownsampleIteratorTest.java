package timely.sample.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.*;
import java.util.Map.Entry;

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
import timely.model.ObjectSizeOf;
import timely.model.Tag;
import timely.auth.VisibilityCache;
import timely.model.parse.TagListParser;
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

    private void createTestData1() {
        List<Tag> tags = Collections.singletonList(new Tag("host", "host1"));

        for (long i = 0; i < 1000; i += 100) {
            Metric m = new Metric("sys.loadAvg", i, .2, tags);
            Mutation mutation = MetricAdapter.toMutation(m);
            for (ColumnUpdate cu : mutation.getUpdates()) {
                Key key = new Key(mutation.getRow(), cu.getColumnFamily(), cu.getColumnQualifier(),
                        cu.getColumnVisibility(), cu.getTimestamp());
                System.out.println(key.toString());
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

    private void createTestData2() {
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
        Map<Set<Tag>, Downsample> samples = runQuery(iter, testData1, 100, -1);
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
        Map<Set<Tag>, Downsample> samples = runQuery(iter, testData2, 100, -1);
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
        Map<Set<Tag>, Downsample> samples = runQuery(iter, testData2, 200, -1);
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

    static public class SampleObject implements ObjectSizeOf {

        private long sizeInBytes = 0;

        public void setSizeInBytes(long sizeInBytes) {
            this.sizeInBytes = sizeInBytes;
        }

        @Override
        public long sizeInBytes() {
            return sizeInBytes;
        }
    }

    @Test
    public void memoryEstimatorTestSmallObjects() {
        long maxMemory = 1000;
        long start = System.currentTimeMillis();
        long period = 500l;
        long sizeOfObjects = 20;
        SampleObject o = new SampleObject();
        DownsampleMemoryEstimator memoryEstimator = new DownsampleMemoryEstimator(maxMemory, start, period);
        boolean shouldReturn = false;
        for (long x = 100; x <= 5000; x += 100) {
            long timestamp = start + x;
            o.setSizeInBytes(o.sizeInBytes() + sizeOfObjects);
            shouldReturn = memoryEstimator.shouldReturnBasedOnMemoryUsage(timestamp, o);
            if (memoryEstimator.isNewBucket()) {
                long memoryPercentageUsedCalculated = Math.round((double) o.sizeInBytes() / maxMemory * 100);
                long memoryPercentageUsedEstimate = Math.round(memoryEstimator.getMemoryUsedPercentage());
                long percentError = Math.round(Math.abs(memoryPercentageUsedCalculated - memoryPercentageUsedEstimate)
                        / memoryPercentageUsedCalculated * 100);
                assertTrue(percentError == 0);
            }

            if (shouldReturn) {
                o.setSizeInBytes(0);
                memoryEstimator.reset();
            }
        }
        assertTrue(shouldReturn);
    }

    @Test
    public void memoryEstimatorTestLargeObjects() {
        long maxMemory = 10000;
        long start = System.currentTimeMillis();
        long period = 500l;
        long sizeOfObjects = 200;
        SampleObject o = new SampleObject();
        DownsampleMemoryEstimator memoryEstimator = new DownsampleMemoryEstimator(maxMemory, start, period);
        boolean shouldReturn = false;
        for (long x = 100; x <= 5000; x += 100) {
            long timestamp = start + x;
            o.setSizeInBytes(o.sizeInBytes() + sizeOfObjects);
            shouldReturn = memoryEstimator.shouldReturnBasedOnMemoryUsage(timestamp, o);
            if (memoryEstimator.isNewBucket()) {
                long memoryPercentageUsedCalculated = Math.round((double) o.sizeInBytes() / maxMemory * 100);
                long memoryPercentageUsedEstimate = Math.round(memoryEstimator.getMemoryUsedPercentage());
                long percentError = Math.round(Math.abs(memoryPercentageUsedCalculated - memoryPercentageUsedEstimate)
                        / memoryPercentageUsedCalculated * 100);
                assertTrue(percentError == 0);
                assertTrue(memoryEstimator.isHighVolumeBuckets());
            }

            if (shouldReturn) {
                o.setSizeInBytes(0);
                memoryEstimator.reset();
            }
        }
        assertTrue(shouldReturn);
    }

    private SortedMap<Key, Value> createTestData3(int elapsedTime, int skipInterval, int numTagVariations) {
        SortedMap<Key, Value> testData3 = new TreeMap<>();
        List<List<Tag>> listOfTagVariations = new ArrayList<>();
        for (int x = 1; x <= numTagVariations; x++) {
            listOfTagVariations.add(Collections.singletonList(new Tag("instance", Integer.toString(x))));
        }

        for (long i = 0; i < elapsedTime; i += skipInterval) {
            for (List<Tag> tags : listOfTagVariations) {
                put(testData3, new Metric("sys.loadAvg", i, i * 2, tags));
            }
        }
        return testData3;
    }

    @Test
    public void testDownsampleCombining() throws Exception {

        int numTagVariations = 2;
        int sampleInterval = 50;
        int elapsedTime = 100;
        int skipInterval = 10;
        SortedMap<Key, Value> testData3 = createTestData3(elapsedTime, skipInterval, numTagVariations);
        DownsampleIterator iter = new DownsampleIterator();
        Map<Set<Tag>, Downsample> samples = runQuery(iter, testData3, sampleInterval, 1000);
        assertEquals(numTagVariations, samples.size());
        long totalBuckets = 0;
        for (Entry<Set<Tag>, Downsample> entry : samples.entrySet()) {
            totalBuckets = totalBuckets + entry.getValue().getNumBuckets();
        }
        assertEquals((elapsedTime / sampleInterval) * numTagVariations, totalBuckets);
    }

    private Map<Set<Tag>, Downsample> runQuery(SortedKeyValueIterator<Key, Value> iter, SortedMap<Key, Value> testData,
            long period, long maxDownsampleMemory) throws Exception {
        IteratorSetting is = new IteratorSetting(100, DownsampleIterator.class);
        DownsampleIterator.setDownsampleOptions(is, 0, 1000, period, maxDownsampleMemory, Avg.class.getName());
        SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(testData);
        iter.init(source, is.getOptions(), null);
        iter.seek(new Range(), Collections.emptyList(), true);
        boolean hasTop = iter.hasTop();
        assertTrue(hasTop);
        Key key = null;
        Map<Set<Tag>, Downsample> samples = new HashMap<>();
        do {
            Map<Set<Tag>, Downsample> currentSamples = DownsampleIterator.decodeValue(iter.getTopValue());
            List<Downsample> downsampleArray = new ArrayList<>();
            for (Entry<Set<Tag>, Downsample> entry : currentSamples.entrySet()) {
                Downsample downsample = samples.get(entry.getKey());
                if (downsample == null) {
                    samples.put(entry.getKey(), entry.getValue());
                } else {
                    downsampleArray.clear();
                    downsampleArray.add(downsample);
                    downsampleArray.add(entry.getValue());
                    samples.put(entry.getKey(), Downsample.combineDownsample(downsampleArray, null));
                }
            }
            key = iter.getTopKey();
            System.out.println(key.toString());
        } while (iter.hasTop());

        assertEquals(testData.lastKey(), key);
        return samples;
    }

}

package timely.server.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SortedMapIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import timely.accumulo.MetricAdapter;
import timely.model.Metric;
import timely.model.parse.TagListParser;

public class TagFilterTest {

    private static final Long TEST_TIME = System.currentTimeMillis();
    private static TagListParser tagListParser = new TagListParser();
    private SortedMap<Key,Value> table;
    private SortedKeyValueIterator<Key,Value> source;

    @Before
    public void setup() {
        table = new TreeMap<>();
        List<Map<String,String>> tagMapList = new ArrayList<>();
        for (int rack = 1; rack <= 10; rack++) {
            for (int node = 1; node <= 10; node++) {
                Map<String,String> tags = new LinkedHashMap<>();
                tags.put("cluster", "metrics");
                tags.put("host", String.format("r%02dn%02d", rack, node));
                tags.put("rack", String.format("r%02d", rack));
                tagMapList.add(tags);
            }
        }

        for (int i = 0; i < 100; i++) {
            table.put(MetricAdapter.toKey("sys.cpu.user", tagMapList.get(i), TEST_TIME), new Value(MetricAdapter.encodeValue(0.0)));
            table.put(MetricAdapter.toKey("sys.cpu.system", tagMapList.get(i), TEST_TIME), new Value(MetricAdapter.encodeValue(0.0)));
            table.put(MetricAdapter.toKey("sys.cpu.idle", tagMapList.get(i), TEST_TIME), new Value(MetricAdapter.encodeValue(0.0)));
        }
        source = new SortedMapIterator(table);
    }

    @Test
    public void testHostRangeRegex() throws Exception {
        Map<String,String> queryTags = new HashMap<>();
        queryTags.put("rack", "r07");
        // this is also testing the escaping of the commas in the regex when a tagList is created
        queryTags.put("host", "r07n0[1,3,5,7,9]");
        // 3 metrics with 5 matching tags each
        runTest(queryTags, 15);
    }

    @Test
    public void testHostRegex() throws Exception {
        Map<String,String> queryTags = new HashMap<>();
        queryTags.put("rack", "r07");
        queryTags.put("host", "r07n.*");
        // 3 metrics with 10 matching tags each
        runTest(queryTags, 30);
    }

    @Test
    public void testHostWildcard() throws Exception {
        Map<String,String> queryTags = new HashMap<>();
        queryTags.put("rack", "r07");
        queryTags.put("host", ".*");
        // 3 metrics with 10 matching tags each
        runTest(queryTags, 30);
    }

    @Test
    public void testHostOrRegex() throws Exception {
        Map<String,String> queryTags = new HashMap<>();
        queryTags.put("rack", "r08");
        queryTags.put("host", "r08n01|r08n02|r08n03|r08n04");
        // 3 metrics with 4 matching tags each
        runTest(queryTags, 12);
    }

    @Test
    public void testNoMatch() throws Exception {
        Map<String,String> queryTags = new HashMap<>();
        queryTags.put("rack", "r09");
        queryTags.put("host", "r08n01|r08n02|r08n03|r08n04");
        // 3 metrics with 0 matching tags each (different racks)
        runTest(queryTags, 0);
    }

    public void runTest(Map<String,String> queryTags, int expectedResults) throws Exception {
        TagFilter iter = new TagFilter();
        HashMap<String,String> options = new HashMap<>();
        options.put(TagFilter.TAGS, TagFilter.serializeTags(queryTags));
        options.put(TagFilter.CACHE_SIZE, Integer.toString(10));
        iter.init(source, options, null);
        iter.setMaxCacheSize(1000);
        iter.seek(new Range(), Collections.singleton(new ArrayByteSequence("cluster=metrics".getBytes())), true);
        int results = 0;
        while (iter.hasTop()) {
            Key k = iter.getTopKey();
            Value v = iter.getTopValue();
            Metric m = MetricAdapter.parse(k, v);
            HashMap<String,String> tags = new HashMap<>();
            m.getTags().stream().forEach(tag -> tags.put(tag.getKey(), tag.getValue()));
            for (Map.Entry<String,String> entry : queryTags.entrySet()) {
                Assert.assertTrue(tags.get(entry.getKey()).matches(entry.getValue()));
            }
            results++;
            iter.next();
        }
        Assert.assertEquals(expectedResults, results);
    }

    @Test
    public void testSerialization() {
        TagFilter tagFilter = new TagFilter();
        Map<String,String> requestedTags = new LinkedHashMap<>();
        requestedTags.put("cluster", "metrics");
        requestedTags.put("host", "r07n0[1,3,5,7,9]");
        requestedTags.put("rack", "r07");
        String tagListString = tagFilter.serializeTags(requestedTags);
        Assert.assertEquals("cluster=metrics\0host=r07n0[1,3,5,7,9]\0rack=r07", tagListString);
        Assert.assertEquals(requestedTags, tagFilter.deserializeTags(tagListString));
    }

}

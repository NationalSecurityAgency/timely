package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.Decompressor;
import fi.iki.yak.ts.compression.gorilla.GorillaDecompressor;
import fi.iki.yak.ts.compression.gorilla.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import timely.Configuration;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.model.Metric;
import timely.model.Tag;
import timely.model.Value;

import java.util.*;

public class TestGorillaStore {

    static private Configuration configuration = null;

    @BeforeClass
    public static void setup() {
        configuration = new Configuration();
        configuration.getSecurity().setAllowAnonymousAccess(true);
    }

    private MetricMemoryStore getMetricMemoryStore1() throws TimelyException {

        MetricMemoryStore mmStore = new MetricMemoryStore(configuration);

        long timestamp = 10000;
        Map<String, String> tags = new HashMap<>();
        tags.put("part", "webservice");
        tags.put("host", "host1");
        mmStore.store(createMetric("mymetric", tags, 2.1, timestamp += 100));
        mmStore.store(createMetric("mymetric", tags, 2.3, timestamp += 100));
        mmStore.store(createMetric("mymetric", tags, 2.0, timestamp += 100));
        mmStore.store(createMetric("mymetric", tags, 2.8, timestamp += 100));
        tags.put("host", "host2");
        mmStore.store(createMetric("mymetric", tags, 2.9, timestamp += 100));
        mmStore.store(createMetric("mymetric", tags, 2.4, timestamp += 100));
        mmStore.store(createMetric("mymetric", tags, 2.2, timestamp += 100));
        mmStore.store(createMetric("mymetric", tags, 2.7, timestamp += 100));

        return mmStore;

    }

    @Test
    public void testOne() {

        GorillaStore gStore = new GorillaStore();

        long now = System.currentTimeMillis();
        gStore.addValue(now += 100, 1.123);
        gStore.addValue(now += 100, 2.314);
        gStore.addValue(now += 100, 3.856);
        gStore.addValue(now += 100, 4.7678);
        gStore.addValue(now += 100, 5.8966);
        gStore.addValue(now += 100, 6.0976);
        gStore.addValue(now += 100, 1.2345);

        GorillaDecompressor decompressor = gStore.getDecompressor();
        Pair pair = null;
        while ((pair = decompressor.readPair()) != null) {
            System.out.println(pair.getTimestamp() + " --> " + pair.getDoubleValue());
        }

        System.out.println("---------------");

        gStore.addValue(now += 100, 2.3456);
        gStore.addValue(now += 100, 3.4567);

        decompressor = gStore.getDecompressor();
        pair = null;
        while ((pair = decompressor.readPair()) != null) {
            System.out.println(pair.getTimestamp() + " --> " + pair.getDoubleValue());
        }

    }

    private Metric createMetric(String metric, Map<String, String> tags, double value, long timestamp) {
        Metric m = new Metric();
        m.setName(metric);
        List<Tag> tagList = new ArrayList<>();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            tagList.add(new Tag(entry.getKey(), entry.getValue()));
        }
        m.setTags(tagList);
        Value metricValue = new Value();
        metricValue.setMeasure(value);
        metricValue.setTimestamp(timestamp);
        m.setValue(metricValue);
        return m;
    }

}

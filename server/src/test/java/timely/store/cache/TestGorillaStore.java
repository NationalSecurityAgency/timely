package timely.store.cache;

import fi.iki.yak.ts.compression.gorilla.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import timely.Configuration;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
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

        List<WrappedGorillaDecompressor> decompressorList = gStore.getDecompressors(0, Long.MAX_VALUE);
        Pair pair = null;
        for (WrappedGorillaDecompressor w : decompressorList) {
            while ((pair = w.readPair()) != null) {
                System.out.println(pair.getTimestamp() + " --> " + pair.getDoubleValue());
            }
        }

        System.out.println("---------------");

        gStore.addValue(now += 100, 2.3456);
        gStore.addValue(now += 100, 3.4567);

        decompressorList = gStore.getDecompressors(0, Long.MAX_VALUE);
        pair = null;
        for (WrappedGorillaDecompressor w : decompressorList) {
            while ((pair = w.readPair()) != null) {
                System.out.println(pair.getTimestamp() + " --> " + pair.getDoubleValue());
            }
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

    @Test
    public void testExtentOfStorage() {

        GorillaStore gStore = new GorillaStore();

        HashMap<String, String> tags = new HashMap<>();
        tags.put("host", "localhost");

        long start = System.currentTimeMillis();
        long timestamp = start;

        for (int x = 1; x <= 100; x++) {

            System.out.println("adding value x:" + x);
            gStore.addValue(timestamp, 2.0);
            timestamp = timestamp + 1000;

            if (x % 10 == 0) {
                gStore.archiveCurrentCompressor();
            }
            if (x < 50) {
                continue;
            }

            System.out.println("fetching values x:" + x);
            long totalObservations = 0;

            List<WrappedGorillaDecompressor> decompressorList = gStore.getDecompressors(start, timestamp);
            Pair pair = null;
            for (WrappedGorillaDecompressor w : decompressorList) {
                while ((pair = w.readPair()) != null) {
                    totalObservations++;
                    // System.out.println(pair.getTimestamp() + " --> " +
                    // pair.getDoubleValue());
                }
            }

            Assert.assertEquals("Unexpected number of total observations", x, totalObservations);

        }

    }

}

package timely.store.memory;

import fi.iki.yak.ts.compression.gorilla.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import timely.Configuration;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.model.Metric;
import timely.model.ObjectSizeOf;
import timely.model.Tag;
import timely.model.Value;

import java.util.*;

public class TestMetricMemoryStore {

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
        double[] values = new double[] { 2.1, 2.3, 2.0, 2.8, 4.2, 2.1, 7.2, 4.1, 10.2 };
        for (double d : values) {
            mmStore.store(createMetric("mymetric", tags, d, timestamp));
            timestamp += 1000;
        }

        tags.put("host", "host2");
        values = new double[] { 2.9, 2.4, 2.2, 2.7, 4.2, 2.1, 7.2, 4.1, 10.2 };
        for (double d : values) {
            mmStore.store(createMetric("mymetric", tags, d, timestamp));
            timestamp += 1000;
        }

        tags.put("part", "ingest");
        tags.put("host", "host3");
        values = new double[] { 2.9, 2.4, 2.2, 2.7, 4.2, 2.1, 7.2, 4.1, 10.2 };
        for (double d : values) {
            mmStore.store(createMetric("mymetric", tags, d, timestamp));
            timestamp += 1000;
        }
        return mmStore;

    }

    private MetricMemoryStore getMetricMemoryStore2() throws TimelyException {

        MetricMemoryStore mmStore = new MetricMemoryStore(configuration);

        Map<String, String> tags = new HashMap<>();
        tags.put("part", "webservice");
        tags.put("host", "r01n01");
        tags.put("rack", "r01n01");
        tags.put("instance", "sample");

        boolean debug = false;
        System.out.println("bytesUsed=" + ObjectSizeOf.Sizer.getObjectSize(mmStore, true, debug));

        Random r = new Random();
        long timestamp = 0;
        for (int x = 0; x <= 60 * 24; x++) {
            mmStore.store(createMetric("metric.number.1", tags, r.nextInt(1000), timestamp + (x * 1000)));
            if (x == 0) {
                System.out.println("bytesUsed=" + ObjectSizeOf.Sizer.getObjectSize(mmStore, true, debug));
            }
            // mmStore.store(createMetric("metric.number.2", tags,
            // r.nextInt(1000), timestamp + (x * 1000)));
            // mmStore.store(createMetric("metric.number.3", tags,
            // r.nextInt(1000), timestamp + (x * 1000)));
            // mmStore.store(createMetric("metric.number.4", tags,
            // r.nextInt(1000), timestamp + (x * 1000)));
            // mmStore.store(createMetric("metric.number.5", tags,
            // r.nextInt(1000), timestamp + (x * 1000)));
        }
        System.out.println("bytesUsed=" + ObjectSizeOf.Sizer.getObjectSize(mmStore, true, debug));
        return mmStore;
    }

    @Test
    public void testOne() throws TimelyException {

        MetricMemoryStore mmStore = getMetricMemoryStore1();

        QueryRequest query = new QueryRequest();
        query.setStart(10000);
        query.setEnd(100000);
        query.setMsResolution(true);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setDownsample(Optional.of("5s-avg"));
        subQuery.setMetric("mymetric");
        subQuery.addTag("host", ".*");
        query.setQueries(Collections.singleton(subQuery));

        try {
            List<QueryResponse> responseList = mmStore.query(query);
            for (QueryResponse response : responseList) {
                System.out.println(response.toString());
            }
        } catch (TimelyException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStorage() throws TimelyException {

        MetricMemoryStore mmStore = getMetricMemoryStore2();

        QueryRequest query = new QueryRequest();
        query.setStart(0);
        query.setEnd(86400000);
        query.setMsResolution(true);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setDownsample(Optional.of("5m-avg"));
        subQuery.setMetric("metric.number.1");
        subQuery.addTag("host", ".*");
        query.setQueries(Collections.singleton(subQuery));

        try {
            List<QueryResponse> responseList = mmStore.query(query);
            for (QueryResponse response : responseList) {
                System.out.println(response.toString());
            }
        } catch (TimelyException e) {
            e.printStackTrace();
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

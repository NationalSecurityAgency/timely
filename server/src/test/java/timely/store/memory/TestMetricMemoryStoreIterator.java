package timely.store.memory;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.client.lexicoder.impl.ByteUtils;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;
import org.junit.BeforeClass;
import org.junit.Test;
import timely.Configuration;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.model.Metric;
import timely.model.ObjectSizeOf;
import timely.model.Tag;
import timely.model.Value;
import timely.sample.Aggregation;
import timely.sample.Downsample;
import timely.sample.Sample;
import timely.sample.aggregators.Avg;
import timely.sample.iterators.AggregationIterator;
import timely.sample.iterators.DownsampleIterator;

import java.io.IOException;
import java.util.*;

public class TestMetricMemoryStoreIterator {

    static private Configuration configuration = null;

    @BeforeClass
    public static void setup() {
        configuration = new Configuration();
        configuration.getSecurity().setAllowAnonymousAccess(true);
    }

    private MetricMemoryStore getMetricMemoryStore() throws TimelyException {

        MetricMemoryStore mmStore = new MetricMemoryStore(configuration);

        Map<String, String> tags = new HashMap<>();
        tags.put("part", "webservice");
        tags.put("host", "r01n01");
        tags.put("rack", "r01n01");
        tags.put("instance", "sample");

        Random r = new Random();
        long timestamp = 1000;
        for (int x=0; x <= 60 * 24; x++) {
            mmStore.store(createMetric("metric.number.1", tags, r.nextInt(1000), timestamp + (x * 1000)));
            if (x == 0) {
            }
//            mmStore.store(createMetric("metric.number.2", tags, r.nextInt(1000), timestamp + (x * 1000)));
//            mmStore.store(createMetric("metric.number.3", tags, r.nextInt(1000), timestamp + (x * 1000)));
//            mmStore.store(createMetric("metric.number.4", tags, r.nextInt(1000), timestamp + (x * 1000)));
//            mmStore.store(createMetric("metric.number.5", tags, r.nextInt(1000), timestamp + (x * 1000)));
        }
        return mmStore;
    }

    private static final PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(),
            new LongLexicoder());

    @Test
    public void test2() {

        byte[] bytes = rowCoder.encode(new ComparablePair<String, Long>("row", 0l));



        Key k = new Key(new Text(bytes), new Text("colFam"), new Text("colQual"));
        Pair<String, Long> pair = MetricAdapter.decodeRowKey(k);
        System.out.println(pair.getFirst() + " - " + pair.getSecond());
    }

    @Test
    public void testIterator() throws TimelyException {

        MetricMemoryStore mmStore = getMetricMemoryStore();

        QueryRequest query = new QueryRequest();
        query.setStart(0);
        query.setEnd(86400000);
        query.setMsResolution(true);
        QueryRequest.SubQuery subQuery = new QueryRequest.SubQuery();
        subQuery.setDownsample(Optional.of("5m-avg"));
        subQuery.setMetric("metric.number.1");
        subQuery.addTag("host", ".*");
        query.setQueries(Collections.singleton(subQuery));

        VisibilityFilter visFilter = new VisibilityFilter(new Authorizations());
        MetricMemoryStoreIterator itr = new MetricMemoryStoreIterator(mmStore, visFilter, subQuery, 0, 86400000);

        Map<String, String> downSampleOptions = new HashMap<>();
        downSampleOptions.put(DownsampleIterator.START, Long.toString(0));
        downSampleOptions.put(DownsampleIterator.END, Long.toString(86400000));
        downSampleOptions.put(DownsampleIterator.PERIOD, Long.toString(AccumuloConfiguration.getTimeInMillis("5m")));
        downSampleOptions.put(DownsampleIterator.AGGCLASS, Avg.class.getName());
        DownsampleIterator dsi = new DownsampleIterator();

        try {
            dsi.init(itr, downSampleOptions, null);
            dsi.seek(new Range(subQuery.getMetric()), null, true);
            while (dsi.hasTop()) {
                dsi.next();
                Map<Set<Tag>, Aggregation> aggregations = AggregationIterator.decodeValue(dsi.getTopValue());
                for (Map.Entry<Set<Tag>, Aggregation> entry : aggregations.entrySet()) {
                    for (Sample s : entry.getValue()) {
                        System.out.println(entry.toString() + " --> " + s.toString());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
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

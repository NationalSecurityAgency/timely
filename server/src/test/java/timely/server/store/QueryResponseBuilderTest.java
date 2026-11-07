package timely.server.store;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.model.Tag;
import timely.server.sample.Aggregation;
import timely.server.sample.Downsample;
import timely.server.sample.aggregators.Avg;
import timely.server.sample.aggregators.Sum;

public class QueryResponseBuilderTest {

    private SubQuery query(Double min, Double max) {
        SubQuery query = new SubQuery();
        query.setMetric("metric");
        query.setMinValue(min);
        query.setMaxValue(max);
        return query;
    }

    private Aggregation samples() {
        Aggregation values = new Aggregation(new Sum());
        values.add(1000, -2);
        values.add(2000, 0);
        values.add(3000, 2);
        values.add(4000, 4);
        return values;
    }

    @Test
    public void unboundedResponseRetainsMetadataAndSamples() {
        QueryResponse result = QueryResponseBuilder.build(query(null, null), Collections.singleton(new Tag("host", "a")), Collections.singleton(samples()),
                        1000);
        Assert.assertEquals("metric", result.getMetric());
        Assert.assertEquals("a", result.getTags().get("host"));
        Assert.assertEquals(4, result.getDps().size());
        Assert.assertEquals(-2.0, result.getDps().get("1"));
    }

    @Test
    public void rangeKeepsBothEndpoints() {
        QueryResponse result = QueryResponseBuilder.build(query(0.0, 2.0), Collections.emptySet(), Collections.singleton(samples()), 1);
        Assert.assertEquals(2, result.getDps().size());
        Assert.assertEquals(0.0, result.getDps().get("2000"));
        Assert.assertEquals(2.0, result.getDps().get("3000"));
    }

    @Test
    public void lowerBoundFiltersFinalValues() {
        QueryResponse result = QueryResponseBuilder.build(query(2.0, null), Collections.emptySet(), Collections.singleton(samples()), 1000);
        Assert.assertEquals(2, result.getDps().size());
        Assert.assertEquals(2.0, result.getDps().get("3"));
        Assert.assertEquals(4.0, result.getDps().get("4"));
    }

    @Test
    public void upperBoundFiltersFinalValues() {
        QueryResponse result = QueryResponseBuilder.build(query(null, 0.0), Collections.emptySet(), Collections.singleton(samples()), 1000);
        Assert.assertEquals(2, result.getDps().size());
        Assert.assertEquals(-2.0, result.getDps().get("1"));
        Assert.assertEquals(0.0, result.getDps().get("2"));
    }

    @Test
    public void fullyFilteredSeriesKeepsMetadataWithEmptyDps() {
        QueryResponse result = QueryResponseBuilder.build(query(10.0, null), Collections.singleton(new Tag("host", "a")), Collections.singleton(samples()),
                        1000);
        Assert.assertEquals("metric", result.getMetric());
        Assert.assertEquals("a", result.getTags().get("host"));
        Assert.assertTrue(result.getDps().isEmpty());
    }

    @Test
    public void filteringHappensAfterCombiningCacheAndStoreSamples() {
        Aggregation cached = new Aggregation(new Sum());
        cached.add(1000, 4);
        Aggregation stored = new Aggregation(new Sum());
        stored.add(1000, 4);
        QueryResponse result = QueryResponseBuilder.build(query(7.0, 9.0), Collections.emptySet(), Arrays.asList(cached, stored), 1000);
        Assert.assertEquals(Collections.singletonMap("1", 8.0), result.getDps());
    }

    @Test
    public void filteringDoesNotRemoveInputsToDownsampling() {
        Downsample downsample = new Downsample(1000, 2000, 1000, new Avg());
        downsample.add(1100, 5);
        downsample.add(1200, 15);
        QueryResponse result = QueryResponseBuilder.build(query(9.0, 11.0), Collections.emptySet(), Collections.singleton(downsample), 1000);
        Assert.assertEquals(Collections.singletonMap("1", 10.0), result.getDps());
    }

    @Test
    public void fractionalBoundsAndMillisecondTimestampsAreRetained() {
        Aggregation values = new Aggregation(new Sum());
        values.add(1234, 1.5);
        values.add(1235, 1.6);
        QueryResponse result = QueryResponseBuilder.build(query(1.5, 1.5), Collections.emptySet(), Collections.singleton(values), 1);
        Assert.assertEquals(Collections.singletonMap("1234", 1.5), result.getDps());
    }

}

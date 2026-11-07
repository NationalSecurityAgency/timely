package timely.api.request.timeseries;

import org.junit.Assert;
import org.junit.Test;

import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.util.JsonUtil;

public class QueryValueBoundsTest {

    private QueryRequest request(Double min, Double max) {
        SubQuery sub = new SubQuery();
        sub.setMetric("sys.cpu.user");
        sub.setAggregator("avg");
        sub.setMinValue(min);
        sub.setMaxValue(max);
        QueryRequest request = new QueryRequest();
        request.addQuery(sub);
        return request;
    }

    @Test
    public void unboundedQueriesKeepAllValues() {
        QueryRequest request = request(null, null);
        request.validate();
        SubQuery sub = request.getQueries().iterator().next();
        for (double value : new double[] {-10, 0, 10, Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY}) {
            Assert.assertTrue(sub.acceptsValue(value));
        }
    }

    @Test
    public void lowerBoundIsInclusive() {
        SubQuery sub = request(-2.0, null).getQueries().iterator().next();
        Assert.assertFalse(sub.acceptsValue(-3));
        Assert.assertTrue(sub.acceptsValue(-2));
        Assert.assertTrue(sub.acceptsValue(0));
        Assert.assertFalse(sub.acceptsValue(Double.NaN));
    }

    @Test
    public void upperBoundIsInclusive() {
        SubQuery sub = request(null, 2.0).getQueries().iterator().next();
        Assert.assertTrue(sub.acceptsValue(-3));
        Assert.assertTrue(sub.acceptsValue(2));
        Assert.assertFalse(sub.acceptsValue(3));
        Assert.assertFalse(sub.acceptsValue(Double.NaN));
    }

    @Test
    public void equalBoundsKeepOnlyMatchingValues() {
        QueryRequest request = request(0.0, 0.0);
        request.validate();
        SubQuery sub = request.getQueries().iterator().next();
        Assert.assertTrue(sub.acceptsValue(0));
        Assert.assertTrue(sub.acceptsValue(-0.0));
        Assert.assertFalse(sub.acceptsValue(-1));
        Assert.assertFalse(sub.acceptsValue(1));
    }

    @Test
    public void invalidBoundsAreRejected() {
        Double[][] bounds = {{2.0, 1.0}, {Double.NaN, null}, {null, Double.NaN}, {Double.POSITIVE_INFINITY, null}, {Double.NEGATIVE_INFINITY, null},
                {null, Double.POSITIVE_INFINITY}, {null, Double.NEGATIVE_INFINITY}};
        for (Double[] bound : bounds) {
            Assert.assertThrows(IllegalArgumentException.class, () -> request(bound[0], bound[1]).validate());
        }
    }

    @Test
    public void postBoundsAreParsedPerSubquery() throws Exception {
        QueryRequest request = (QueryRequest) new QueryRequest().parseBody("{\"start\":1000,\"queries\":["
                        + "{\"metric\":\"a\",\"aggregator\":\"avg\",\"minValue\":-2,\"maxValue\":5}," + "{\"metric\":\"b\",\"aggregator\":\"sum\"}]}");
        request.validate();
        SubQuery[] queries = request.getQueries().toArray(new SubQuery[0]);
        Assert.assertEquals(Double.valueOf(-2), queries[0].getMinValue());
        Assert.assertEquals(Double.valueOf(5), queries[0].getMaxValue());
        Assert.assertNull(queries[1].getMinValue());
        Assert.assertNull(queries[1].getMaxValue());
    }

    @Test
    public void getBoundsApplyToEverySubquery() throws Exception {
        QueryRequest request = (QueryRequest) new QueryRequest()
                        .parseQueryParameters(new QueryStringDecoder("/api/query?start=1000&m=avg:a&m=sum:b&minValue=-2&maxValue=5"));
        request.validate();
        Assert.assertEquals(2, request.getQueries().size());
        for (SubQuery sub : request.getQueries()) {
            Assert.assertEquals(Double.valueOf(-2), sub.getMinValue());
            Assert.assertEquals(Double.valueOf(5), sub.getMaxValue());
        }
    }

    @Test
    public void absentBoundsDoNotChangeSerializedSubquery() throws Exception {
        SubQuery sub = request(null, null).getQueries().iterator().next();
        String json = JsonUtil.getObjectMapper().writeValueAsString(sub);
        Assert.assertFalse(json.contains("minValue"));
        Assert.assertFalse(json.contains("maxValue"));
        Assert.assertFalse(json.contains("acceptsValue"));
    }

    @Test
    public void boundsSurviveJsonRoundTrip() throws Exception {
        SubQuery original = request(-2.0, 5.0).getQueries().iterator().next();
        String json = JsonUtil.getObjectMapper().writeValueAsString(original);
        SubQuery copy = JsonUtil.getObjectMapper().readValue(json, SubQuery.class);
        Assert.assertEquals(original, copy);
        Assert.assertEquals(original.hashCode(), copy.hashCode());
        copy.setMinValue(-1.0);
        Assert.assertNotEquals(original, copy);
        copy.setMinValue(-2.0);
        copy.setMaxValue(6.0);
        Assert.assertNotEquals(original, copy);
    }

    @Test
    public void websocketBoundsAreParsed() throws Exception {
        QueryRequest request = (QueryRequest) new QueryRequest()
                        .parseBody("{\"operation\":\"query\",\"start\":1000," + "\"queries\":[{\"metric\":\"a\",\"aggregator\":\"sum\",\"minValue\":1.5}]}");
        request.validate();
        Assert.assertEquals(Double.valueOf(1.5), request.getQueries().iterator().next().getMinValue());
    }

    @Test
    public void invalidGetBoundsAreRejected() throws Exception {
        for (String bounds : new String[] {"minValue=NaN", "maxValue=Infinity", "minValue=2&maxValue=1"}) {
            QueryRequest request = (QueryRequest) new QueryRequest().parseQueryParameters(new QueryStringDecoder("/api/query?start=1000&m=avg:a&" + bounds));
            Assert.assertThrows(IllegalArgumentException.class, request::validate);
        }
        Assert.assertThrows(NumberFormatException.class,
                        () -> new QueryRequest().parseQueryParameters(new QueryStringDecoder("/api/query?start=1000&m=avg:a&minValue=abc")));
    }

}

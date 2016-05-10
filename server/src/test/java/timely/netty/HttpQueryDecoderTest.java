package timely.netty;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import timely.api.Request;
import timely.api.model.Tag;
import timely.api.query.request.AggregatorsRequest;
import timely.api.query.request.QueryRequest;
import timely.api.query.request.QueryRequest.Filter;
import timely.api.query.request.QueryRequest.RateOption;
import timely.api.query.request.QueryRequest.SubQuery;
import timely.api.query.request.SearchLookupRequest;
import timely.api.query.request.SuggestRequest;
import timely.api.query.response.TimelyException;
import timely.netty.http.HttpQueryDecoder;

import com.fasterxml.jackson.databind.JsonMappingException;

public class HttpQueryDecoderTest {

    private HttpQueryDecoder decoder = null;

    @Before
    public void setup() {
        decoder = new HttpQueryDecoder();
    }

    @Test(expected = TimelyException.class)
    public void testUnknownURI() throws Exception {
        decoder.parseURI("/api/uknown");
    }

    @Test
    public void testAggregatorsURI() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/aggregators");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(AggregatorsRequest.class, requests.iterator().next().getClass());
    }

    @Test
    public void testAggregatorsPost() throws Exception {
        Collection<Request> requests = decoder.parsePOST("/api/aggregators", null);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(AggregatorsRequest.class, requests.iterator().next().getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupURIWithNoArgs() throws Exception {
        decoder.parseURI("/api/search/lookup");
    }

    @Test(expected = JsonMappingException.class)
    public void testLookupPostWithNoArgs() throws Exception {
        decoder.parsePOST("/api/search/lookup", "");
    }

    @Test
    public void testLookupURIWithNoLimit() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/search/lookup?m=sys.cpu.user");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SearchLookupRequest.class, requests.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) requests.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(25, lookup.getLimit());
        Assert.assertEquals(0, lookup.getTags().size());
    }

    @Test
    public void testLookupPostWithNoLimit() throws Exception {
        // @formatter:off
        String content = 
        "{\n" +
        "    \"metric\": \"sys.cpu.user\"\n" + 
        "}";
        // @formatter:on
        Collection<Request> requests = decoder.parsePOST("/api/search/lookup", content);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SearchLookupRequest.class, requests.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) requests.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(25, lookup.getLimit());
        Assert.assertEquals(0, lookup.getTags().size());
    }

    @Test
    public void testLookupURIWithLimit() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/search/lookup?m=sys.cpu.user&limit=3000");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SearchLookupRequest.class, requests.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) requests.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(3000, lookup.getLimit());
        Assert.assertEquals(0, lookup.getTags().size());
    }

    @Test
    public void testLookupPostWithLimit() throws Exception {
        // @formatter:off
        String content = 
        "{\n" +
        "    \"metric\": \"sys.cpu.user\",\n" +
        "    \"limit\": 3000\n" +
        "}";
        // @formatter:on
        Collection<Request> requests = decoder.parsePOST("/api/search/lookup", content);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SearchLookupRequest.class, requests.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) requests.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(3000, lookup.getLimit());
        Assert.assertEquals(0, lookup.getTags().size());
    }

    @Test
    public void testLookupURIWithLimitAndTags() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/search/lookup?m=sys.cpu.user{host=*}&limit=3000");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SearchLookupRequest.class, requests.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) requests.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(3000, lookup.getLimit());
        Assert.assertEquals(1, lookup.getTags().size());
        Tag tag = lookup.getTags().iterator().next();
        Assert.assertEquals("host", tag.getKey());
        Assert.assertEquals("*", tag.getValue());
    }

    @Test
    public void testLookupPostWithLimitAndTags() throws Exception {
        // @formatter:off
        String content = 
        "{\n" +
        "    \"metric\": \"sys.cpu.user\",\n" +
        "    \"limit\": 3000,\n" +
        "    \"tags\":[\n" +
        "        {\n" +
        "            \"key\": \"host\",\n" +
        "            \"value\": \"*\"\n" +
        "        }\n"+
        "    ]\n" +
        "}";
        // @formatter:on
        Collection<Request> requests = decoder.parsePOST("/api/search/lookup", content);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SearchLookupRequest.class, requests.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) requests.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(3000, lookup.getLimit());
        Assert.assertEquals(1, lookup.getTags().size());
        Tag tag = lookup.getTags().iterator().next();
        Assert.assertEquals("host", tag.getKey());
        Assert.assertEquals("*", tag.getValue());
    }

    @Test
    public void testSuggestURIWithInvalidType() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/suggest?type=foo");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SuggestRequest.class, requests.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) requests.iterator().next();
        Assert.assertEquals("foo", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSuggestURIValidateWithInvalidTypeFails() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/suggest?type=foo");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SuggestRequest.class, requests.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) requests.iterator().next();
        Assert.assertEquals("foo", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestURIWithValidType() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/suggest?type=metrics");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SuggestRequest.class, requests.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) requests.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestPostWithValidType() throws Exception {
        // @formatter:off
        String content = 
        "{\n" +
        "    \"type\": \"metrics\"\n" +
        "}";
        // @formatter:on
        Collection<Request> requests = decoder.parsePOST("/api/suggest", content);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SuggestRequest.class, requests.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) requests.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestWithValidTypeAndQuery() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/suggest?type=metrics&q=sys.cpu.user");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SuggestRequest.class, requests.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) requests.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertEquals("sys.cpu.user", suggest.getQuery().get());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestPostWithValidTypeAndQuery() throws Exception {
        // @formatter:off
        String content = 
        "{\n" +
        "    \"type\": \"metrics\",\n" +
        "    \"q\": \"sys.cpu.user\"\n" +
        "}";
        // @formatter:on
        Collection<Request> requests = decoder.parsePOST("/api/suggest", content);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SuggestRequest.class, requests.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) requests.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertEquals("sys.cpu.user", suggest.getQuery().get());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestWithValidTypeAndQueryAndMax() throws Exception {
        Collection<Request> requests = decoder.parseURI("/api/suggest?type=metrics&q=sys.cpu.user&max=30");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SuggestRequest.class, requests.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) requests.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertEquals("sys.cpu.user", suggest.getQuery().get());
        Assert.assertEquals(30, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestPostWithValidTypeAndQueryAndMax() throws Exception {
        // @formatter:off
        String content = 
        "{\n" +
        "    \"type\": \"metrics\",\n" +
        "    \"q\": \"sys.cpu.user\",\n" +
        "    \"max\": 30\n" +
        "}";
        // @formatter:on
        Collection<Request> requests = decoder.parsePOST("/api/suggest", content);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(SuggestRequest.class, requests.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) requests.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertEquals("sys.cpu.user", suggest.getQuery().get());
        Assert.assertEquals(30, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testQueryURIAll() throws Exception {
        Collection<Request> requests = decoder
                .parseURI("/api/query?start=1356998400&end=1356998460&m=sum:rate{false,100,0}:sys.cpu.user{host=*}{rack=r1|r2}&tsuid=sum:000001000002000042,000001000002000043");
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(QueryRequest.class, requests.iterator().next().getClass());
        QueryRequest query = (QueryRequest) requests.iterator().next();
        Assert.assertEquals(1356998400, query.getStart());
        Assert.assertEquals(1356998460, query.getEnd());
        Assert.assertEquals(2, query.getQueries().size());
        Iterator<SubQuery> iter = query.getQueries().iterator();
        SubQuery first = iter.next();
        Assert.assertEquals(true, first.isMetricQuery());
        Assert.assertEquals(false, first.isTsuidQuery());
        Assert.assertEquals("sum", first.getAggregator());
        Assert.assertEquals("sys.cpu.user", first.getMetric());
        Assert.assertEquals(true, first.isRate());
        RateOption firstRateOption = first.getRateOptions();
        Assert.assertEquals(false, firstRateOption.isCounter());
        Assert.assertEquals(100, firstRateOption.getCounterMax());
        Assert.assertEquals(0, firstRateOption.getResetValue());
        Assert.assertEquals(false, first.getDownsample().isPresent());
        Assert.assertEquals(1, first.getTags().size());
        Iterator<Entry<String, String>> tags = first.getTags().entrySet().iterator();
        Entry<String, String> firstTag = tags.next();
        Assert.assertEquals("rack", firstTag.getKey());
        Assert.assertEquals("r1|r2", firstTag.getValue());
        Assert.assertEquals(1, first.getFilters().size());
        Iterator<Filter> filters = first.getFilters().iterator();
        Filter firstFilter = filters.next();
        Assert.assertEquals(null, firstFilter.getType());
        Assert.assertEquals("host", firstFilter.getTagk());
        Assert.assertEquals("*", firstFilter.getFilter());
        Assert.assertEquals(true, firstFilter.isGroupBy());
        SubQuery second = iter.next();
        Assert.assertEquals(false, second.isMetricQuery());
        Assert.assertEquals(true, second.isTsuidQuery());
        Assert.assertEquals(2, second.getTsuids().size());
        Iterator<String> tsuids = second.getTsuids().iterator();
        Assert.assertEquals("000001000002000042", tsuids.next());
        Assert.assertEquals("000001000002000043", tsuids.next());
        query.validate();
    }

    @Test
    public void testQueryPostAll() throws Exception {
        // @formatter:off
        String content =
        "{\n"+
        "    \"start\": 1356998400,\n"+
        "    \"end\": 1356998460,\n"+
        "    \"queries\": [\n"+
        "        {\n"+
        "            \"aggregator\": \"sum\",\n"+
        "            \"metric\": \"sys.cpu.user\",\n"+
        "            \"rate\": \"true\",\n"+
        "            \"rateOptions\": \n"+
        "                {\"counter\":false,\"counterMax\":100,\"resetValue\":0},\n"+
        "            \"tags\": {\n"+
        "                   \"host\": \"*\",\n" +
        "                   \"rack\": \"r1\"\n" +
        "            },\n"+
        "            \"filters\": [\n"+
        "                {\n"+
        "                   \"type\":\"wildcard\",\n"+
        "                   \"tagk\":\"host\",\n"+
        "                   \"filter\":\"*\",\n"+
        "                   \"groupBy\":true\n"+
        "                },\n"+
        "                {\n"+
        "                   \"type\":\"literal_or\",\n"+
        "                   \"tagk\":\"rack\",\n"+
        "                   \"filter\":\"r1|r2\",\n"+
        "                   \"groupBy\":false\n"+
        "                }\n"+
        "            ]\n"+
        "        },\n"+
        "        {\n"+
        "            \"aggregator\": \"sum\",\n"+
        "            \"tsuids\": [\n"+
        "                \"000001000002000042\",\n"+
        "                \"000001000002000043\"\n"+
        "            ]\n"+
        "        }\n"+
        "    ]\n"+
        "}";
        // @formatter:on
        Collection<Request> requests = decoder.parsePOST("/api/query", content);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(QueryRequest.class, requests.iterator().next().getClass());
        QueryRequest query = (QueryRequest) requests.iterator().next();
        Assert.assertEquals(1356998400, query.getStart());
        Assert.assertEquals(1356998460, query.getEnd());
        Assert.assertEquals(2, query.getQueries().size());
        Iterator<SubQuery> iter = query.getQueries().iterator();
        SubQuery first = iter.next();
        Assert.assertEquals(true, first.isMetricQuery());
        Assert.assertEquals(false, first.isTsuidQuery());
        Assert.assertEquals("sum", first.getAggregator());
        Assert.assertEquals("sys.cpu.user", first.getMetric());
        Assert.assertEquals(true, first.isRate());
        RateOption firstRateOption = first.getRateOptions();
        Assert.assertEquals(false, firstRateOption.isCounter());
        Assert.assertEquals(100, firstRateOption.getCounterMax());
        Assert.assertEquals(0, firstRateOption.getResetValue());
        Assert.assertEquals(false, first.getDownsample().isPresent());
        Assert.assertEquals(2, first.getTags().size());
        Iterator<Entry<String, String>> tags = first.getTags().entrySet().iterator();
        Entry<String, String> firstTag = tags.next();
        Assert.assertEquals("host", firstTag.getKey());
        Assert.assertEquals("*", firstTag.getValue());
        Entry<String, String> secondTag = tags.next();
        Assert.assertEquals("rack", secondTag.getKey());
        Assert.assertEquals("r1", secondTag.getValue());
        Assert.assertEquals(2, first.getFilters().size());
        Iterator<Filter> filters = first.getFilters().iterator();
        Filter firstFilter = filters.next();
        Assert.assertEquals("wildcard", firstFilter.getType());
        Assert.assertEquals("host", firstFilter.getTagk());
        Assert.assertEquals("*", firstFilter.getFilter());
        Assert.assertEquals(true, firstFilter.isGroupBy());
        Filter secondFilter = filters.next();
        Assert.assertEquals("literal_or", secondFilter.getType());
        Assert.assertEquals("rack", secondFilter.getTagk());
        Assert.assertEquals("r1|r2", secondFilter.getFilter());
        Assert.assertEquals(false, secondFilter.isGroupBy());
        SubQuery second = iter.next();
        Assert.assertEquals(false, second.isMetricQuery());
        Assert.assertEquals(true, second.isTsuidQuery());
        Assert.assertEquals(2, second.getTsuids().size());
        Iterator<String> tsuids = second.getTsuids().iterator();
        Assert.assertEquals("000001000002000042", tsuids.next());
        Assert.assertEquals("000001000002000043", tsuids.next());
        query.validate();
    }

    @Test
    public void testQueryPostRateOption() throws Exception {
        // @formatter:off
        String content = 
        "{"
        +  "\"start\":1447767369171,"
        +  "\"queries\":"
        +   "["
        +    "{"
        +       "\"metric\":\"sys.cpu.user\","
        +       "\"aggregator\":\"sum\","
        +       "\"rate\":true,"
        +       "\"rateOptions\":{\"counter\":false},"
        +       "\"downsample\":\"30s-avg\""
        +    "}"
        +  "]"
        + "}";
        // @formatter:on
        Collection<Request> requests = decoder.parsePOST("/api/query", content);
        Assert.assertEquals(1, requests.size());
        Assert.assertEquals(QueryRequest.class, requests.iterator().next().getClass());
        QueryRequest query = (QueryRequest) requests.iterator().next();
        Assert.assertEquals(1447767369171L, query.getStart());
        Assert.assertEquals(1, query.getQueries().size());
        Iterator<SubQuery> iter = query.getQueries().iterator();
        SubQuery first = iter.next();
        Assert.assertEquals(true, first.isMetricQuery());
        Assert.assertEquals(false, first.isTsuidQuery());
        Assert.assertEquals("sum", first.getAggregator());
        Assert.assertEquals("sys.cpu.user", first.getMetric());
        Assert.assertEquals(true, first.isRate());
        RateOption firstRateOption = first.getRateOptions();
        Assert.assertEquals(false, firstRateOption.isCounter());
    }

}

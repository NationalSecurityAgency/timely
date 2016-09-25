package timely.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ClientCookieEncoder;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;

import timely.Configuration;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.MetricRequest;
import timely.model.Metric;
import timely.model.Tag;
import timely.api.request.timeseries.AggregatorsRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.request.timeseries.SuggestRequest;
import timely.api.request.timeseries.QueryRequest.Filter;
import timely.api.request.timeseries.QueryRequest.RateOption;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.request.VersionRequest;
import timely.auth.AuthCache;
import timely.netty.Constants;
import timely.test.TestConfiguration;
import timely.util.JsonUtil;

import com.fasterxml.jackson.databind.JsonMappingException;

public class HttpRequestDecoderTest {

    public static class TestHttpQueryDecoder extends HttpRequestDecoder {

        public TestHttpQueryDecoder(Configuration config) {
            super(config);
        }

        @Override
        public void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
            super.decode(ctx, msg, out);
        }

    }

    private static final Long TEST_TIME = System.currentTimeMillis();

    private static Configuration config = null;
    private static Configuration anonConfig = null;
    private TestHttpQueryDecoder decoder = null;
    private List<Object> results = new ArrayList<>();
    private static String cookie = null;

    @BeforeClass
    public static void before() throws Exception {
        config = TestConfiguration.createMinimalConfigurationForTest();
        anonConfig = TestConfiguration.createMinimalConfigurationForTest();
        anonConfig.getSecurity().setAllowAnonymousAccess(true);
        cookie = URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name());
        AuthCache.setSessionMaxAge(config);
        AuthCache.getCache().put(cookie, new UsernamePasswordAuthenticationToken("test", "test1"));
    }

    @AfterClass
    public static void after() {
        AuthCache.resetSessionMaxAge();
    }

    @Before
    public void setup() throws Exception {
        results.clear();
    }

    private void addCookie(FullHttpRequest request) {
        request.headers().set(Names.COOKIE, ClientCookieEncoder.STRICT.encode(Constants.COOKIE_NAME, cookie));
    }

    @Test
    public void testUnknownURI() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/unknown");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(request, results.get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregatorsURINoSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/aggregators");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(AggregatorsRequest.class, results.iterator().next().getClass());
    }

    @Test
    public void testAggregatorsURIWithAnonAccess() throws Exception {
        decoder = new TestHttpQueryDecoder(anonConfig);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/aggregators");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(AggregatorsRequest.class, results.iterator().next().getClass());
    }

    @Test
    public void testAggregatorsURI() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/aggregators");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(AggregatorsRequest.class, results.iterator().next().getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAggregatorsPostNoSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/aggregators");
        decoder.decode(null, request, results);
    }

    @Test
    public void testAggregatorsPostAnonAccess() throws Exception {
        decoder = new TestHttpQueryDecoder(anonConfig);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/aggregators");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(AggregatorsRequest.class, results.iterator().next().getClass());
    }

    @Test
    public void testAggregatorsPost() throws Exception {
        decoder = new TestHttpQueryDecoder(anonConfig);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/aggregators");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(AggregatorsRequest.class, results.iterator().next().getClass());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupURIWithNoArgs() throws Exception {
        decoder = new TestHttpQueryDecoder(anonConfig);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/search/lookup");
        decoder.decode(null, request, results);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupURIWithNoSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/search/lookup");
        decoder.decode(null, request, results);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLookupURIWithWithSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/search/lookup");
        addCookie(request);
        decoder.decode(null, request, results);
    }

    @Test(expected = JsonMappingException.class)
    public void testLookupPostWithNoArgs() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/search/lookup");
        addCookie(request);
        decoder.decode(null, request, results);
    }

    @Test(expected = JsonMappingException.class)
    public void testLookupPostWithNoSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/search/lookup");
        decoder.decode(null, request, results);
    }

    @Test
    public void testLookupURIWithNoLimit() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/search/lookup?m=sys.cpu.user");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SearchLookupRequest.class, results.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) results.iterator().next();
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
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/search/lookup");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SearchLookupRequest.class, results.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) results.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(25, lookup.getLimit());
        Assert.assertEquals(0, lookup.getTags().size());
    }

    @Test
    public void testLookupURIWithLimit() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/search/lookup?m=sys.cpu.user&limit=3000");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SearchLookupRequest.class, results.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) results.iterator().next();
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
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/search/lookup");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SearchLookupRequest.class, results.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) results.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(3000, lookup.getLimit());
        Assert.assertEquals(0, lookup.getTags().size());
    }

    @Test
    public void testLookupURIWithLimitAndTags() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/search/lookup?m=sys.cpu.user{host=*}&limit=3000");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SearchLookupRequest.class, results.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) results.iterator().next();
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
        "            \"host\":\"*\"\n" +
        "        }\n"+
        "    ]\n" +
        "}";
        // @formatter:on
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/search/lookup");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SearchLookupRequest.class, results.iterator().next().getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) results.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        Assert.assertEquals(3000, lookup.getLimit());
        Assert.assertEquals(1, lookup.getTags().size());
        Tag tag = lookup.getTags().iterator().next();
        Assert.assertEquals("host", tag.getKey());
        Assert.assertEquals("*", tag.getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSuggestNoSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/suggest?type=foo");
        decoder.decode(null, request, results);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSuggestURIWithInvalidTypeAnonAccess() throws Exception {
        decoder = new TestHttpQueryDecoder(anonConfig);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/suggest?type=foo");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("foo", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSuggestURIWithInvalidType() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/suggest?type=foo");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("foo", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSuggestURIValidateWithInvalidTypeFails() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/suggest?type=foo");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("foo", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSuggestURIWithValidTypeNoSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/suggest?type=metrics");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestURIWithValidType() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/suggest?type=metrics");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
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
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/suggest");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertFalse(suggest.getQuery().isPresent());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestWithValidTypeAndQuery() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/suggest?type=metrics&q=sys.cpu.user");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
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
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/suggest");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertEquals("sys.cpu.user", suggest.getQuery().get());
        Assert.assertEquals(25, suggest.getMax());
        suggest.validate();
    }

    @Test
    public void testSuggestWithValidTypeAndQueryAndMax() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET,
                "/api/suggest?type=metrics&q=sys.cpu.user&max=30");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
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
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST,
                "/api/suggest");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.iterator().next().getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertEquals("sys.cpu.user", suggest.getQuery().get());
        Assert.assertEquals(30, suggest.getMax());
        suggest.validate();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testQueryWithNoSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/api/query?start=1356998400&end=1356998460&m=sum:rate{false,100,0}:sys.cpu.user{host=*}{rack=r1|r2}&tsuid=sum:000001000002000042,000001000002000043");
        decoder.decode(null, request, results);
    }

    @Test
    public void testQueryURIAllAnonAccess() throws Exception {
        decoder = new TestHttpQueryDecoder(anonConfig);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/api/query?start=1356998400&end=1356998460&m=sum:rate{false,100,0}:sys.cpu.user{host=*}{rack=r1|r2}&tsuid=sum:000001000002000042,000001000002000043");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(QueryRequest.class, results.iterator().next().getClass());
        QueryRequest query = (QueryRequest) results.iterator().next();
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
    public void testQueryURIAllWithSession() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.GET,
                "/api/query?start=1356998400&end=1356998460&m=sum:rate{false,100,0}:sys.cpu.user{host=*}{rack=r1|r2}&tsuid=sum:000001000002000042,000001000002000043");
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(QueryRequest.class, results.iterator().next().getClass());
        QueryRequest query = (QueryRequest) results.iterator().next();
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
    public void testQueryEmptyTags() throws Exception {
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
                        "            \"tags\":[]" +
                        "        }\n"+
                        "    ]\n"+
                        "}";
        // @formatter:on
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/query");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(QueryRequest.class, results.iterator().next().getClass());
        QueryRequest query = (QueryRequest) results.iterator().next();
        Assert.assertEquals(1356998400, query.getStart());
        Assert.assertEquals(1356998460, query.getEnd());

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
        Assert.assertEquals(100, firstRateOption.getCounterMax());
        Assert.assertEquals(0, firstRateOption.getResetValue());
        Assert.assertEquals(false, first.getDownsample().isPresent());
        Assert.assertEquals(0, first.getTags().size());
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
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/query");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(QueryRequest.class, results.iterator().next().getClass());
        QueryRequest query = (QueryRequest) results.iterator().next();
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
    public void testQueryPostGlobalAnnotations() throws Exception {
        // @formatter:off
        String content = "" +
        "{"
        + "\"start\":1447767369171,"
        + "\"queries\":"
        +  "["
        +   "{"
        +      "\"metric\":\"sys.cpu.user\","
        +      "\"aggregator\":\"sum\","
        +      "\"downsample\":\"30s-avg\""
        +   "}"
        +  "],"
        + "\"msResolution\":false,"
        + "\"globalAnnotations\":true,"
        + "\"showQuery\":true"
        + "}";
        // @formatter:on
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/query");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(QueryRequest.class, results.iterator().next().getClass());
        QueryRequest query = (QueryRequest) results.iterator().next();
        Assert.assertEquals(1447767369171L, query.getStart());
        Assert.assertEquals(false, query.isMsResolution());
        Assert.assertEquals(true, query.isGlobalAnnotations());
        Assert.assertEquals(true, query.isShowQuery());
        Assert.assertEquals(1, query.getQueries().size());
        Iterator<SubQuery> iter = query.getQueries().iterator();
        SubQuery first = iter.next();
        Assert.assertEquals(true, first.isMetricQuery());
        Assert.assertEquals(false, first.isTsuidQuery());
        Assert.assertEquals("sum", first.getAggregator());
        Assert.assertEquals("sys.cpu.user", first.getMetric());
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
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/query");
        request.content().writeBytes(content.getBytes());
        addCookie(request);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(QueryRequest.class, results.iterator().next().getClass());
        QueryRequest query = (QueryRequest) results.iterator().next();
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

    @Test
    public void testVersionGet() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/version");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(VersionRequest.class, results.iterator().next().getClass());
    }

    @Test
    public void testVersionPost() throws Exception {
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/version");
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(VersionRequest.class, results.iterator().next().getClass());
    }

    @Test
    public void testPutMetricPost() throws Exception {
        // @formatter:off
        Metric m = Metric.newBuilder()
                .name("sys.cpu.user")
                .value(TEST_TIME, 1.0D)
                .tag(new Tag("tag1", "value1"))
                .tag(new Tag("tag2", "value2"))
                .build();
        // @formatter:on
        byte[] buf = JsonUtil.getObjectMapper().writeValueAsBytes(m);
        decoder = new TestHttpQueryDecoder(config);
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/put");
        request.content().writeBytes(buf);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(MetricRequest.class, results.iterator().next().getClass());
    }

}

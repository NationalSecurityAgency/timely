package timely.netty.websocket;

import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;

import timely.Configuration;
import timely.api.model.Metric;
import timely.api.request.VersionRequest;
import timely.api.request.subscription.AddSubscription;
import timely.api.request.subscription.CloseSubscription;
import timely.api.request.subscription.CreateSubscription;
import timely.api.request.subscription.RemoveSubscription;
import timely.api.request.timeseries.AggregatorsRequest;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.request.timeseries.SuggestRequest;
import timely.auth.AuthCache;
import timely.test.CaptureChannelHandlerContext;
import timely.test.TestConfiguration;

public class WebSocketRequestDecoderTest {

    private static final Long TEST_TIME = System.currentTimeMillis();

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static Configuration config = null;
    private static Configuration anonConfig = null;
    private WebSocketRequestDecoder decoder = null;
    private List<Object> results = new ArrayList<>();
    private static String cookie = null;

    @BeforeClass
    public static void before() throws Exception {
        File conf = temp.newFile("config.properties");
        conf.deleteOnExit();
        TestConfiguration cfg = TestConfiguration.createMinimalConfigurationForTest();
        cfg.toConfiguration(conf);
        config = new Configuration(conf);
        File conf2 = temp.newFile("anon-config.properties");
        conf2.deleteOnExit();
        TestConfiguration cfg2 = TestConfiguration.createMinimalConfigurationForTest();
        cfg2.put(Configuration.ALLOW_ANONYMOUS_ACCESS, "true");
        cfg2.toConfiguration(conf2);
        anonConfig = new Configuration(conf2);
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

    @Test
    public void testVersion() throws Exception {
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{ \"operation\" : \"version\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(null, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(VersionRequest.class, results.get(0).getClass());
    }

    @Test
    public void testPutMetric() throws Exception {
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{" + "\"operation\" : \"put\",\n" + "\"metric\" : \"sys.cpu.user\",\n" + "\"timestamp\":"
                + TEST_TIME + ",\n" + "\"value\":1.0,\n" + "\"tags\":[ {\n" + "\"key\":\"tag1\",\n"
                + "\"value\":\"value1\"\n" + "},{\n" + "\"key\":\"tag2\",\n" + "\"value\":\"value2\"\n" + "}]\n" + "}";

        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(null, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(Metric.class, results.get(0).getClass());
        Metric metric = (Metric) results.iterator().next();
        Assert.assertEquals("sys.cpu.user", metric.getMetric());
        Assert.assertEquals(TEST_TIME, (Long) metric.getTimestamp());
        Assert.assertEquals(1.0D, metric.getValue(), 0.0D);
        Assert.assertEquals(2, metric.getTags().size());
    }

    @Test
    public void testCreateSubscriptionWithSessionIdAndNonAnonymousAccess() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(config);
        String request = "{ \"operation\" : \"create\", \"sessionId\" : \"1234\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertNotNull(ctx.msg);
        Assert.assertEquals(CloseWebSocketFrame.class, ctx.msg.getClass());
        Assert.assertEquals(1008, ((CloseWebSocketFrame) ctx.msg).statusCode());
        Assert.assertEquals("User must log in", ((CloseWebSocketFrame) ctx.msg).reasonText());
    }

    @Test
    public void testCreateSubscriptionWithValidSessionIdAndNonAnonymousAccess() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(config);
        String request = "{ \"operation\" : \"create\", \"sessionId\" : \"" + cookie + "\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(CreateSubscription.class, results.get(0).getClass());
    }

    @Test
    public void testCreateSubscriptionWithoutSessionId() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{ \"operation\" : \"create\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertNotNull(ctx.msg);
        Assert.assertEquals(CloseWebSocketFrame.class, ctx.msg.getClass());
        Assert.assertEquals(1008, ((CloseWebSocketFrame) ctx.msg).statusCode());
        Assert.assertEquals("SessionID is null, must log in first", ((CloseWebSocketFrame) ctx.msg).reasonText());
    }

    @Test
    public void testCreateSubscription() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{ \"operation\" : \"create\", \"sessionId\" : \"1234\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(CreateSubscription.class, results.get(0).getClass());
        CreateSubscription create = (CreateSubscription) results.iterator().next();
        create.validate();
    }

    @Test
    public void testAddSubscription() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{ \"operation\" : \"add\", \"sessionId\" : \"1234\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(AddSubscription.class, results.get(0).getClass());
        AddSubscription add = (AddSubscription) results.iterator().next();
        add.validate();
    }

    @Test
    public void testRemoveSubscription() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{ \"operation\" : \"remove\", \"sessionId\" : \"1234\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(RemoveSubscription.class, results.get(0).getClass());
        RemoveSubscription remove = (RemoveSubscription) results.iterator().next();
        remove.validate();
    }

    @Test
    public void testCloseSubscription() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{ \"operation\" : \"close\", \"sessionId\" : \"1234\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(CloseSubscription.class, results.get(0).getClass());
        CloseSubscription close = (CloseSubscription) results.iterator().next();
        close.validate();
    }

    @Test
    public void testAggregrators() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{ \"operation\" : \"aggregators\", \"sessionId\" : \"1234\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(AggregatorsRequest.class, results.get(0).getClass());
        AggregatorsRequest agg = (AggregatorsRequest) results.iterator().next();
        agg.validate();
    }

    @Test
    public void testMetrics() throws Exception {
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        String request = "{ \"operation\" : \"metrics\", \"sessionId\" : \"1234\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(MetricsRequest.class, results.get(0).getClass());
        MetricsRequest metrics = (MetricsRequest) results.iterator().next();
        metrics.validate();
    }

    @Test
    public void testQuery() throws Exception {
        // @formatter:off
        String request =
        "{\n"+
        "	 \"operation\" : \"query\",\n"+
        "    \"sessionId\" : \"1234\",\n"+
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
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(QueryRequest.class, results.get(0).getClass());
        QueryRequest query = (QueryRequest) results.iterator().next();
        query.validate();
    }

    @Test
    public void testLookup() throws Exception {
        String request = "{ \"operation\" : \"lookup\", \"sessionId\" : \"1234\", \"metric\" : \"sys.cpu.user\" }";
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SearchLookupRequest.class, results.get(0).getClass());
        SearchLookupRequest lookup = (SearchLookupRequest) results.iterator().next();
        Assert.assertEquals("sys.cpu.user", lookup.getQuery());
        lookup.validate();
    }

    @Test
    public void testSuggest() throws Exception {
        // @formatter:off
    	String request = 
    			"{\n" +
    			"    \"operation\" : \"suggest\",\n" +
    	        "    \"sessionId\" : \"1234\",\n" +
    	        "    \"type\": \"metrics\",\n" +
    	        "    \"q\": \"sys.cpu.user\",\n" +
    	        "    \"max\": 30\n" +    			
    			"}";
    	// @formatter:on
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        decoder = new WebSocketRequestDecoder(anonConfig);
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.get(0).getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertEquals("sys.cpu.user", suggest.getQuery().get());
        Assert.assertEquals(30, suggest.getMax());
        suggest.validate();
    }
}

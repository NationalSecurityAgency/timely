package timely.server.integration;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import io.netty.channel.Channel;
import io.netty.channel.local.LocalChannel;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import timely.api.request.MetricRequest;
import timely.api.request.VersionRequest;
import timely.api.request.timeseries.AggregatorsRequest;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.request.timeseries.SuggestRequest;
import timely.api.request.websocket.AddSubscription;
import timely.api.request.websocket.CloseSubscription;
import timely.api.request.websocket.CreateSubscription;
import timely.api.request.websocket.RemoveSubscription;
import timely.auth.SubjectIssuerDNPair;
import timely.auth.TimelyPrincipal;
import timely.auth.TimelyUser;
import timely.common.component.AuthenticationService;
import timely.common.configuration.SecurityProperties;
import timely.model.Metric;
import timely.netty.websocket.WebSocketRequestDecoder;
import timely.netty.websocket.subscription.SubscriptionConstants;
import timely.server.test.CaptureChannelHandlerContext;
import timely.server.test.TestConfiguration;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class WebSocketRequestDecoderIT {

    public static class WebSocketCaptureChannelHandlerContext extends CaptureChannelHandlerContext {

        private Channel channel = new LocalChannel();

        @Override
        public Channel channel() {
            return channel;
        }
    }

    private static final Long TEST_TIME = (System.currentTimeMillis() / 1000) * 1000;

    private SecurityProperties requireUserSecurity = TestConfiguration.requireUserSecurity();
    private SecurityProperties anonymousSecurity = TestConfiguration.anonymousSecurity();
    private WebSocketRequestDecoder decoder = null;
    private List<Object> results = new ArrayList<>();
    private String cookie = null;
    private CaptureChannelHandlerContext ctx = new WebSocketCaptureChannelHandlerContext();

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private AuthenticationService authenticationService;

    @Autowired
    private SecurityProperties securityProperties;

    @Before
    public void before() throws Exception {
        cookie = URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name());
        TimelyUser timelyUser = new TimelyUser(SubjectIssuerDNPair.of("ANONYMOUS"), TimelyUser.UserType.USER, securityProperties.getRequiredAuths(),
                        securityProperties.getRequiredRoles(), null, -1L);
        authenticationService.getAuthCache().put(cookie, new TimelyPrincipal(timelyUser));
        results.clear();
        ctx = new WebSocketCaptureChannelHandlerContext();
    }

    @Test
    public void testVersion() throws Exception {
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
    // @formatter:off
        String request = "{ "+
          "\"operation\" : \"version\"" +
        " }";
        // @formatter:on
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(VersionRequest.class, results.get(0).getClass());
    }

    @Test
    public void testPutMetric() throws Exception {
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
    // @formatter:off
        String request = "{" +
          "\"operation\" : \"put\",\n" +
          "\"name\" : \"sys.cpu.user\",\n" +
          "\"timestamp\":" + TEST_TIME + ",\n" +
          "\"measure\":1.0,\n" +
          "\"tags\":[ {\n" +
              "\"key\":\"tag1\",\n" +
              "\"value\":\"value1\"\n" +
          "},{\n" +
              "\"key\":\"tag2\",\n" +
              "\"value\":\"value2\"\n" +
          "}]\n" +
        "}";
        // @formatter:on
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(MetricRequest.class, results.get(0).getClass());
        Metric metric = ((MetricRequest) results.iterator().next()).getMetric();
        Assert.assertEquals("sys.cpu.user", metric.getName());
        Assert.assertEquals(TEST_TIME, metric.getValue().getTimestamp());
        Assert.assertEquals(1.0D, metric.getValue().getMeasure(), 0.0D);
        Assert.assertEquals(2, metric.getTags().size());
    }

    @Test
    public void testCreateSubscriptionWithMissingSessionId() throws Exception {
        decoder = new WebSocketRequestDecoder(authenticationService, requireUserSecurity);
    // @formatter:off
        String request = "{ "+
          "\"operation\" : \"create\", " +
          "\"subscriptionId\" : \"1234\"" +
        " }";
        // @formatter:on
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertNotNull(ctx.msg);
        Assert.assertEquals(CloseWebSocketFrame.class, ctx.msg.getClass());
        Assert.assertEquals(1008, ((CloseWebSocketFrame) ctx.msg).statusCode());
        Assert.assertEquals("User must authenticate", ((CloseWebSocketFrame) ctx.msg).reasonText());
    }

    @Test
    public void testCreateSubscriptionWithInvalidSessionIdAndNonAnonymousAccess() throws Exception {
        ctx.channel().attr(SubscriptionConstants.SESSION_ID_ATTR).set(URLEncoder.encode(UUID.randomUUID().toString(), StandardCharsets.UTF_8.name()));
        decoder = new WebSocketRequestDecoder(authenticationService, requireUserSecurity);
    // @formatter:off
        String request = "{ "+
          "\"operation\" : \"create\", " +
          "\"subscriptionId\" : \"1234\"" +
        " }";
        // @formatter:on
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertNotNull(ctx.msg);
        Assert.assertEquals(CloseWebSocketFrame.class, ctx.msg.getClass());
        Assert.assertEquals(1008, ((CloseWebSocketFrame) ctx.msg).statusCode());
        Assert.assertEquals("User must authenticate", ((CloseWebSocketFrame) ctx.msg).reasonText());
    }

    @Test
    public void testCreateSubscriptionWithValidSessionIdAndNonAnonymousAccess() throws Exception {
        ctx.channel().attr(SubscriptionConstants.SESSION_ID_ATTR).set(cookie);
        decoder = new WebSocketRequestDecoder(authenticationService, requireUserSecurity);
    // @formatter:off
        String request = "{ " +
          "\"operation\" : \"create\"," +
          "\"subscriptionId\" : \"" + cookie + "\"" +
        "}";
        // @formatter:on
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(CreateSubscription.class, results.get(0).getClass());
    }

    @Test
    public void testCreateSubscriptionWithoutSubscriptionId() throws Exception {
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
        String request = "{ \"operation\" : \"create\" }";
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertNotNull(ctx.msg);
        Assert.assertEquals(CloseWebSocketFrame.class, ctx.msg.getClass());
        Assert.assertEquals(1008, ((CloseWebSocketFrame) ctx.msg).statusCode());
        Assert.assertEquals("Subscription ID is required.", ((CloseWebSocketFrame) ctx.msg).reasonText());
    }

    @Test
    public void testCreateSubscription() throws Exception {
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
        String request = "{ \"operation\" : \"create\", \"subscriptionId\" : \"1234\" }";
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
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
        String request = "{ \"operation\" : \"add\", \"subscriptionId\" : \"1234\" }";
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
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
        String request = "{ \"operation\" : \"remove\", \"subscriptionId\" : \"1234\" }";
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
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
        String request = "{ \"operation\" : \"close\", \"subscriptionId\" : \"1234\" }";
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
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
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
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
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
        "     \"operation\" : \"query\",\n"+
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
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
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
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
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
                "    \"m\": \"sys.cpu.user\",\n" +
                "    \"max\": 30\n" +
                "}";
        // @formatter:on
        decoder = new WebSocketRequestDecoder(authenticationService, anonymousSecurity);
        TextWebSocketFrame frame = new TextWebSocketFrame();
        frame.content().writeBytes(request.getBytes(StandardCharsets.UTF_8));
        decoder.decode(ctx, frame, results);
        Assert.assertEquals(1, results.size());
        Assert.assertEquals(SuggestRequest.class, results.get(0).getClass());
        SuggestRequest suggest = (SuggestRequest) results.iterator().next();
        Assert.assertEquals("metrics", suggest.getType());
        Assert.assertEquals("sys.cpu.user", suggest.getMetric().get());
        Assert.assertEquals(30, suggest.getMax());
        suggest.validate();
    }
}

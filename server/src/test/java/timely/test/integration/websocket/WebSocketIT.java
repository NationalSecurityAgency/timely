package timely.test.integration.websocket;

import com.fasterxml.jackson.databind.JavaType;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.cookie.ClientCookieEncoder;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import timely.Server;
import timely.api.request.VersionRequest;
import timely.api.request.subscription.AddSubscription;
import timely.api.request.subscription.CloseSubscription;
import timely.api.request.subscription.CreateSubscription;
import timely.api.request.subscription.RemoveSubscription;
import timely.api.request.timeseries.AggregatorsRequest;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.MetricResponse;
import timely.api.response.timeseries.AggregatorsResponse;
import timely.api.response.timeseries.QueryResponse;
import timely.api.response.timeseries.SearchLookupResponse;
import timely.api.response.timeseries.SearchLookupResponse.Result;
import timely.api.response.timeseries.SuggestResponse;
import timely.auth.AuthCache;
import timely.model.Metric;
import timely.model.Tag;
import timely.netty.Constants;
import timely.test.IntegrationTest;
import timely.test.TestConfiguration;
import timely.test.integration.OneWaySSLBase;
import timely.util.JsonUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class WebSocketIT extends OneWaySSLBase {

    private static final Logger LOG = LoggerFactory.getLogger(WebSocketIT.class);
    private static final Long TEST_TIME = System.currentTimeMillis() - (240 * 1000);
    private static final int WS_PORT = 54323;
    private static final URI LOCATION;
    static {
        try {
            LOCATION = new URI("wss://127.0.0.1:" + WS_PORT + "/websocket");
        } catch (URISyntaxException e) {
            throw new RuntimeException("Error creating uri for wss://127.0.0.1:" + WS_PORT + "/websocket", e);
        }
    }

    private static class ClientHandler extends SimpleChannelInboundHandler<Object> {

        private static final Logger LOG = LoggerFactory.getLogger(ClientHandler.class);
        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;
        private List<String> responses = new ArrayList<>();
        private volatile boolean connected = false;

        public ClientHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("Client connected.");
            handshaker.handshake(ctx.channel());
            this.connected = true;
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("Client disconnected.");
            this.connected = false;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOG.error("Error", cause);
            if (!this.handshakeFuture.isDone()) {
                this.handshakeFuture.setFailure(cause);
            }
            ctx.close();
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
            handshakeFuture = ctx.newPromise();
        }

        public List<String> getResponses() {
            List<String> result = null;
            synchronized (responses) {
                result = new ArrayList<>(responses);
                responses.clear();
            }
            return result;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
            LOG.info("Received msg: {}", msg);
            if (!this.handshaker.isHandshakeComplete()) {
                this.handshaker.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                LOG.info("Client connected.");
                this.connected = true;
                this.handshakeFuture.setSuccess();
                return;
            }
            if (msg instanceof FullHttpResponse) {
                throw new IllegalStateException("Unexpected response: " + msg.toString());
            }
            WebSocketFrame frame = (WebSocketFrame) msg;
            if (frame instanceof TextWebSocketFrame) {
                synchronized (responses) {
                    responses.add(((TextWebSocketFrame) frame).text());
                }
            } else if (frame instanceof PingWebSocketFrame) {
                LOG.info("Returning pong message");
                ctx.writeAndFlush(new PongWebSocketFrame());
            } else if (frame instanceof CloseWebSocketFrame) {
                LOG.info("Received message from server to close the channel.");
                ctx.close();
            } else {
                LOG.warn("Unhandled frame type received: " + frame.getClass());
            }
        }

        public boolean isConnected() {
            return connected;
        }

    }

    @AfterClass
    public static void after() {
        AuthCache.resetSessionMaxAge();
    }

    private EventLoopGroup group = null;
    private Channel ch = null;
    private ClientHandler handler = null;
    private Server s = null;
    private String sessionId = null;

    @Before
    public void setup() throws Exception {
        s = new Server(conf);
        s.run();
        this.sessionId = UUID.randomUUID().toString();
        AuthCache.getCache().put(sessionId, new UsernamePasswordAuthenticationToken("test", "test1"));
        group = new NioEventLoopGroup();
        SslContext ssl = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();

        String cookieVal = ClientCookieEncoder.STRICT.encode(Constants.COOKIE_NAME, sessionId);
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(Names.COOKIE, cookieVal);

        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(LOCATION,
                WebSocketVersion.V13, (String) null, false, headers);
        handler = new ClientHandler(handshaker);
        Bootstrap boot = new Bootstrap();
        boot.group(group).channel(NioSocketChannel.class).handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("ssl", ssl.newHandler(ch.alloc(), "127.0.0.1", WS_PORT));
                ch.pipeline().addLast(new HttpClientCodec());
                ch.pipeline().addLast(new HttpObjectAggregator(8192));
                ch.pipeline().addLast(handler);
            }
        });
        ch = boot.connect("127.0.0.1", WS_PORT).sync().channel();
        // Wait until handshake is complete
        while (!handshaker.isHandshakeComplete()) {
            sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            LOG.debug("Waiting for Handshake to complete");
        }
    }

    @Test
    public void testClientDisappears() throws Exception {
        try {
            final String subscriptionId = "1235";
            CreateSubscription c = new CreateSubscription();
            c.setSubscriptionId(subscriptionId);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(c)));

            // Add some data
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + TEST_TIME
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");

            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            // Add subscription, confirm data
            AddSubscription add = new AddSubscription();
            add.setSubscriptionId(subscriptionId);
            add.setMetric("sys.cpu.user");
            add.setDelayTime(1000L);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(add)));

            List<String> response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }

            Metric first = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D).tag(new Tag("rack", "r1"))
                    .tag(new Tag("tag1", "value1")).tag(new Tag("tag2", "value2")).build();
            Metric second = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D).tag(new Tag("rack", "r2"))
                    .tag(new Tag("tag3", "value3")).build();
            for (String metrics : response) {
                MetricResponse m = JsonUtil.getObjectMapper().readValue(metrics, MetricResponse.class);
                Assert.assertTrue(m.equals(MetricResponse.fromMetric(first, subscriptionId))
                        || m.equals(MetricResponse.fromMetric(second, subscriptionId)));
            }
        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testSubscriptionWorkflow() throws Exception {
        try {
            final String subscriptionId = "1234";
            CreateSubscription c = new CreateSubscription();
            c.setSubscriptionId(subscriptionId);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(c)));

            // Add some data
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + TEST_TIME
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");

            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            // Add subscription, confirm data
            AddSubscription add = new AddSubscription();
            add.setSubscriptionId(subscriptionId);
            add.setMetric("sys.cpu.user");
            add.setDelayTime(1000L);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(add)));

            List<String> response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }

            Metric first = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D).tag(new Tag("rack", "r1"))
                    .tag(new Tag("tag1", "value1")).tag(new Tag("tag2", "value2")).build();
            Metric second = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D).tag(new Tag("rack", "r2"))
                    .tag(new Tag("tag3", "value3")).build();
            for (String metrics : response) {
                MetricResponse m = JsonUtil.getObjectMapper().readValue(metrics, MetricResponse.class);
                Assert.assertTrue(m.equals(MetricResponse.fromMetric(first, subscriptionId))
                        || m.equals(MetricResponse.fromMetric(second, subscriptionId)));
            }

            // Add some more data
            put("sys.cpu.user " + (TEST_TIME + 500) + " 6.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user "
                    + (TEST_TIME + 500) + " 7.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");

            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }

            // confirm data
            first = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME + 500, 6.0D).tag(new Tag("rack", "r1"))
                    .tag(new Tag("tag1", "value1")).tag(new Tag("tag2", "value2")).build();
            second = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME + 500, 7.0D).tag(new Tag("rack", "r2"))
                    .tag(new Tag("tag3", "value3")).build();
            for (String metrics : response) {
                MetricResponse m = JsonUtil.getObjectMapper().readValue(metrics, MetricResponse.class);
                Assert.assertTrue(m.equals(MetricResponse.fromMetric(first, subscriptionId))
                        || m.equals(MetricResponse.fromMetric(second, subscriptionId)));
            }

            // Add subscription
            AddSubscription add2 = new AddSubscription();
            add2.setSubscriptionId(subscriptionId);
            add2.setMetric("sys.cpu.idle");
            add2.setDelayTime(1000L);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(add2)));

            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            // Confirm receipt of all data sent to this point
            response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }
            first = Metric.newBuilder().name("sys.cpu.idle").value(TEST_TIME + 2, 1.0D).tag(new Tag("rack", "r1"))
                    .tag(new Tag("tag3", "value3")).tag(new Tag("tag4", "value4")).build();
            second = Metric.newBuilder().name("sys.cpu.idle").value(TEST_TIME + 2, 3.0D).tag(new Tag("rack", "r2"))
                    .tag(new Tag("tag3", "value3")).tag(new Tag("tag4", "value4")).build();
            Metric third = Metric.newBuilder().name("sys.cpu.idle").value(TEST_TIME + 1000, 1.0D)
                    .tag(new Tag("rack", "r1")).tag(new Tag("tag3", "value3")).tag(new Tag("tag4", "value4")).build();
            Metric fourth = Metric.newBuilder().name("sys.cpu.idle").value(TEST_TIME + 1000, 3.0D)
                    .tag(new Tag("rack", "r2")).tag(new Tag("tag3", "value3")).tag(new Tag("tag4", "value4")).build();
            for (String metrics : response) {
                MetricResponse m = JsonUtil.getObjectMapper().readValue(metrics, MetricResponse.class);
                Assert.assertTrue(m.equals(MetricResponse.fromMetric(first, subscriptionId))
                        || m.equals(MetricResponse.fromMetric(second, subscriptionId))
                        || m.equals(MetricResponse.fromMetric(third, subscriptionId))
                        || m.equals(MetricResponse.fromMetric(fourth, subscriptionId)));
            }

            // Remove subscriptions to metric
            RemoveSubscription remove1 = new RemoveSubscription();
            remove1.setSubscriptionId(subscriptionId);
            remove1.setMetric("sys.cpu.user");
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(remove1)));
            RemoveSubscription remove2 = new RemoveSubscription();
            remove2.setSubscriptionId(subscriptionId);
            remove2.setMetric("sys.cpu.idle");
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(remove2)));

            // Close subscription
            CloseSubscription close = new CloseSubscription();
            close.setSubscriptionId(subscriptionId);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(close)));
        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testWSAggregators() throws Exception {
        try {
            AggregatorsRequest request = new AggregatorsRequest();
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(request)));
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            // Confirm receipt of all data sent to this point
            List<String> response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }
            Assert.assertEquals(1, response.size());
            JsonUtil.getObjectMapper().readValue(response.get(0), AggregatorsResponse.class);
        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testWSMetrics() throws Exception {
        try {
            MetricsRequest request = new MetricsRequest();
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(request)));

            // Confirm receipt of all data sent to this point
            List<String> response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }
            Assert.assertEquals(1, response.size());
            Assert.assertEquals("{\"metrics\":[]}", response.get(0));
        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testWSQuery() throws Exception {
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1000)
                    + " 3.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 2000)
                    + " 2.0 tag1=value1 tag3=value3 viz=secret");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            // @formatter:off
            String request =
            "{"+
            "    \"operation\" : \"query\","+
            "    \"start\": "+TEST_TIME+","+
            "    \"end\": "+(TEST_TIME+6000)+","+
            "    \"queries\": ["+
            "        {"+
            "            \"aggregator\": \"sum\","+
            "            \"metric\": \"sys.cpu.user\","+
            "            \"rate\": \"true\","+
            "            \"rateOptions\": "+
            "                {\"counter\":false,\"counterMax\":100,\"resetValue\":0},"+
            "            \"downsample\":\"1s-max\"," +
            "            \"tags\": {"+
            "                   \"tag1\": \".*\"" +
            "            }"+
            "        }"+
            "    ]"+
            "}";
            // @formatter:on
            ch.writeAndFlush(new TextWebSocketFrame(request));

            // Confirm receipt of all data sent to this point
            List<String> response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }
            assertEquals(1, response.size());

            JavaType type = JsonUtil.getObjectMapper().getTypeFactory()
                    .constructCollectionType(List.class, QueryResponse.class);
            List<QueryResponse> results = JsonUtil.getObjectMapper().readValue(response.get(0), type);

            assertEquals(1, results.size());
            QueryResponse query = results.get(0);
            assertEquals("sys.cpu.user", query.getMetric());
            assertEquals(1, query.getTags().size());
            assertTrue(query.getTags().containsKey("tag1"));
            assertEquals("value1", query.getTags().get("tag1"));
            assertEquals(2, query.getDps().size());

        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testWSLookup() throws Exception {
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.user " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3", "sys.cpu.idle " + (TEST_TIME + 1) + " 1.0 tag3=value3 tag4=value4",
                    "sys.cpu.idle " + (TEST_TIME + 2) + " 1.0 tag3=value3 tag4=value4");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            // @formatter:off
            String request = 
            "{"+
            "   \"operation\" : \"lookup\","+
            "   \"metric\" : \"sys.cpu.idle\","+
            "   \"tags\" : ["+
                   "\"tag3=.*\""+
                "]"+
    	    "}";
            // @formatter:on
            ch.writeAndFlush(new TextWebSocketFrame(request));

            // Confirm receipt of all data sent to this point
            List<String> response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }
            assertEquals(1, response.size());
            SearchLookupResponse r = JsonUtil.getObjectMapper().readValue(response.get(0), SearchLookupResponse.class);
            assertEquals(1, r.getResults().size());
            Result lookupResult = r.getResults().get(0);
            assertEquals(1, lookupResult.getTags().size());
            assertEquals("value3", lookupResult.getTags().get("tag3"));

        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testWSSuggest() throws Exception {
        try {
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2", "sys.cpu.idle " + (TEST_TIME + 1)
                    + " 1.0 tag3=value3 tag4=value4", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4", "zzzz 1234567892 1.0 host=localhost");
            // Latency in TestConfiguration is 2s, wait for it
            sleepUninterruptibly(TestConfiguration.WAIT_SECONDS, TimeUnit.SECONDS);

            // @formatter:off
        	String request = 
        			"{\n" +
        			"    \"operation\" : \"suggest\",\n" +
        	        "    \"type\": \"metrics\",\n" +
        	        "    \"q\": \"sys.cpu.user\",\n" +
        	        "    \"max\": 30\n" +    			
        			"}";
        	// @formatter:on
            ch.writeAndFlush(new TextWebSocketFrame(request));

            // Confirm receipt of all data sent to this point
            List<String> response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }
            assertEquals(1, response.size());
            System.out.println(response.get(0));
            SuggestResponse r = JsonUtil.getObjectMapper().readValue(response.get(0), SuggestResponse.class);
            assertEquals(1, r.getSuggestions().size());
            assertEquals("sys.cpu.user", r.getSuggestions().get(0));
        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testPutMetric() throws Exception {
        try {
            Metric m = Metric.newBuilder().name("sys.cpu.user").value(TEST_TIME, 1.0D).tag(new Tag("tag1", "value1"))
                    .build();
            String json = JsonUtil.getObjectMapper().writeValueAsString(m);
            ch.writeAndFlush(new TextWebSocketFrame(json));
        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testVersion() throws Exception {
        try {
            String request = "{ \"operation\" : \"version\" }";
            ch.writeAndFlush(new TextWebSocketFrame(request));
            // Confirm receipt of all data sent to this point
            List<String> response = handler.getResponses();
            while (response.size() == 0 && handler.isConnected()) {
                LOG.info("Waiting for web socket response");
                sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
                response = handler.getResponses();
            }
            assertEquals(1, response.size());
            assertEquals(VersionRequest.VERSION, response.get(0));
        } finally {
            ch.close().sync();
            s.shutdown();
            group.shutdownGracefully();
        }
    }
}

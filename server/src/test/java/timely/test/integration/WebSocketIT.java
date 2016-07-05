package timely.test.integration;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshaker;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.util.UtilWaitThread;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Server;
import timely.api.model.Metric;
import timely.api.model.Tag;
import timely.api.websocket.AddSubscription;
import timely.api.websocket.CloseSubscription;
import timely.api.websocket.CreateSubscription;
import timely.api.websocket.RemoveSubscription;
import timely.test.IntegrationTest;
import timely.util.JsonUtil;

@Category(IntegrationTest.class)
public class WebSocketIT extends OneWaySSLBaseIT {

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

        public ClientHandler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("Client connected.");
            handshaker.handshake(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            LOG.info("Client disconnected.");
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

    }

    @Test
    public void testClientDisappears() throws Exception {
        final EventLoopGroup group = new NioEventLoopGroup();
        final Server s = new Server(conf);
        try {
            SslContext ssl = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(LOCATION,
                    WebSocketVersion.V13, (String) null, false, (HttpHeaders) new DefaultHttpHeaders());
            final ClientHandler handler = new ClientHandler(handshaker);
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
            Channel ch = boot.connect("127.0.0.1", WS_PORT).sync().channel();
            // Wait until handshake is complete
            while (!handshaker.isHandshakeComplete()) {
                UtilWaitThread.sleep(500L);
            }

            final String sessionId = "1235";
            CreateSubscription c = new CreateSubscription();
            c.setSessionId(sessionId);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(c)));

            // Add some data
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + TEST_TIME
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");

            // Latency in TestConfiguration is 3s, wait for it
            UtilWaitThread.sleep(5000L);

            // Add subscription, confirm data
            AddSubscription add = new AddSubscription();
            add.setSessionId(sessionId);
            add.setMetric("sys.cpu.user");
            add.setDelayTime(1000L);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(add)));

            List<String> response = handler.getResponses();
            while (response.size() == 0) {
                LOG.info("Waiting for web socket response");
                UtilWaitThread.sleep(500L);
                response = handler.getResponses();
            }

            Metric first = new Metric();
            first.setMetric("sys.cpu.user");
            first.setTimestamp(TEST_TIME);
            first.setValue(1.0D);
            List<Tag> firstTags = new ArrayList<>();
            firstTags.add(new Tag("rack", "r1"));
            firstTags.add(new Tag("tag1", "value1"));
            firstTags.add(new Tag("tag2", "value2"));
            first.setTags(firstTags);
            Metric second = new Metric();
            second.setMetric("sys.cpu.user");
            second.setTimestamp(TEST_TIME);
            second.setValue(1.0D);
            List<Tag> secondTags = new ArrayList<>();
            secondTags.add(new Tag("rack", "r2"));
            secondTags.add(new Tag("tag3", "value3"));
            second.setTags(secondTags);
            for (String metrics : response) {
                Metric m = JsonUtil.getObjectMapper().readValue(metrics, Metric.class);
                Assert.assertTrue(m.equals(first) || m.equals(second));
            }

            // Close client channel
            ch.close().sync();

        } finally {
            s.shutdown();
            group.shutdownGracefully();
        }
    }

    @Test
    public void testSubscriptionWorkflow() throws Exception {
        final EventLoopGroup group = new NioEventLoopGroup();
        final Server s = new Server(conf);
        try {
            SslContext ssl = SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build();
            WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(LOCATION,
                    WebSocketVersion.V13, (String) null, false, (HttpHeaders) new DefaultHttpHeaders());
            final ClientHandler handler = new ClientHandler(handshaker);
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
            Channel ch = boot.connect("127.0.0.1", WS_PORT).sync().channel();
            // Wait until handshake is complete
            while (!handshaker.isHandshakeComplete()) {
                UtilWaitThread.sleep(500L);
            }

            final String sessionId = "1234";
            CreateSubscription c = new CreateSubscription();
            c.setSessionId(sessionId);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(c)));

            // Add some data
            put("sys.cpu.user " + TEST_TIME + " 1.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user " + TEST_TIME
                    + " 1.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 2)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");

            // Latency in TestConfiguration is 3s, wait for it
            UtilWaitThread.sleep(5000L);

            // Add subscription, confirm data
            AddSubscription add = new AddSubscription();
            add.setSessionId(sessionId);
            add.setMetric("sys.cpu.user");
            add.setDelayTime(1000L);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(add)));

            List<String> response = handler.getResponses();
            while (response.size() == 0) {
                LOG.info("Waiting for web socket response");
                UtilWaitThread.sleep(500L);
                response = handler.getResponses();
            }

            Metric first = new Metric();
            first.setMetric("sys.cpu.user");
            first.setTimestamp(TEST_TIME);
            first.setValue(1.0D);
            List<Tag> firstTags = new ArrayList<>();
            firstTags.add(new Tag("rack", "r1"));
            firstTags.add(new Tag("tag1", "value1"));
            firstTags.add(new Tag("tag2", "value2"));
            first.setTags(firstTags);
            Metric second = new Metric();
            second.setMetric("sys.cpu.user");
            second.setTimestamp(TEST_TIME);
            second.setValue(1.0D);
            List<Tag> secondTags = new ArrayList<>();
            secondTags.add(new Tag("rack", "r2"));
            secondTags.add(new Tag("tag3", "value3"));
            second.setTags(secondTags);
            for (String metrics : response) {
                Metric m = JsonUtil.getObjectMapper().readValue(metrics, Metric.class);
                Assert.assertTrue(m.equals(first) || m.equals(second));
            }

            // Add some more data
            put("sys.cpu.user " + (TEST_TIME + 500) + " 6.0 tag1=value1 tag2=value2 rack=r1", "sys.cpu.user "
                    + (TEST_TIME + 500) + " 7.0 tag3=value3 rack=r2", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 1.0 tag3=value3 tag4=value4 rack=r1", "sys.cpu.idle " + (TEST_TIME + 1000)
                    + " 3.0 tag3=value3 tag4=value4 rack=r2");

            // Latency in TestConfiguration is 3s, wait for it
            UtilWaitThread.sleep(5000L);

            response = handler.getResponses();
            while (response.size() == 0) {
                LOG.info("Waiting for web socket response");
                UtilWaitThread.sleep(500L);
                response = handler.getResponses();
            }
            ;

            // confirm data
            first = new Metric();
            first.setMetric("sys.cpu.user");
            first.setTimestamp(TEST_TIME + 500);
            first.setValue(6.0D);
            firstTags = new ArrayList<>();
            firstTags.add(new Tag("rack", "r1"));
            firstTags.add(new Tag("tag1", "value1"));
            firstTags.add(new Tag("tag2", "value2"));
            first.setTags(firstTags);
            second = new Metric();
            second.setMetric("sys.cpu.user");
            second.setTimestamp(TEST_TIME + 500);
            second.setValue(7.0D);
            secondTags = new ArrayList<>();
            secondTags.add(new Tag("rack", "r2"));
            secondTags.add(new Tag("tag3", "value3"));
            second.setTags(secondTags);
            for (String metrics : response) {
                Metric m = JsonUtil.getObjectMapper().readValue(metrics, Metric.class);
                Assert.assertTrue(m.equals(first) || m.equals(second));
            }

            // Add subscription
            AddSubscription add2 = new AddSubscription();
            add2.setSessionId(sessionId);
            add2.setMetric("sys.cpu.idle");
            add2.setDelayTime(1000L);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(add2)));

            // Latency in TestConfiguration is 3s, wait for it
            UtilWaitThread.sleep(5000L);

            // Confirm receipt of all data sent to this point
            response = handler.getResponses();
            while (response.size() == 0) {
                LOG.info("Waiting for web socket response");
                UtilWaitThread.sleep(500L);
                response = handler.getResponses();
            }
            ;
            first = new Metric();
            first.setMetric("sys.cpu.idle");
            first.setTimestamp(TEST_TIME + 2);
            first.setValue(1.0D);
            firstTags = new ArrayList<>();
            firstTags.add(new Tag("rack", "r1"));
            firstTags.add(new Tag("tag3", "value3"));
            firstTags.add(new Tag("tag4", "value4"));
            first.setTags(firstTags);
            second = new Metric();
            second.setMetric("sys.cpu.idle");
            second.setTimestamp(TEST_TIME + 2);
            second.setValue(3.0D);
            secondTags = new ArrayList<>();
            secondTags.add(new Tag("rack", "r2"));
            secondTags.add(new Tag("tag3", "value3"));
            secondTags.add(new Tag("tag4", "value4"));
            second.setTags(secondTags);
            Metric third = new Metric();
            third.setMetric("sys.cpu.idle");
            third.setTimestamp(TEST_TIME + 1000);
            third.setValue(1.0D);
            List<Tag> thirdTags = new ArrayList<>();
            thirdTags.add(new Tag("rack", "r1"));
            thirdTags.add(new Tag("tag3", "value3"));
            thirdTags.add(new Tag("tag4", "value4"));
            third.setTags(thirdTags);
            Metric fourth = new Metric();
            fourth.setMetric("sys.cpu.idle");
            fourth.setTimestamp(TEST_TIME + 1000);
            fourth.setValue(3.0D);
            List<Tag> fourthTags = new ArrayList<>();
            fourthTags.add(new Tag("rack", "r2"));
            fourthTags.add(new Tag("tag3", "value3"));
            fourthTags.add(new Tag("tag4", "value4"));
            fourth.setTags(fourthTags);
            for (String metrics : response) {
                Metric m = JsonUtil.getObjectMapper().readValue(metrics, Metric.class);
                Assert.assertTrue(m.equals(first) || m.equals(second) || m.equals(third) || m.equals(fourth));
            }

            // Remove subscriptions to metric
            RemoveSubscription remove1 = new RemoveSubscription();
            remove1.setSessionId(sessionId);
            remove1.setMetric("sys.cpu.user");
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(remove1)));
            RemoveSubscription remove2 = new RemoveSubscription();
            remove2.setSessionId(sessionId);
            remove2.setMetric("sys.cpu.idle");
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(remove2)));

            // Close subscription
            CloseSubscription close = new CloseSubscription();
            close.setSessionId(sessionId);
            ch.writeAndFlush(new TextWebSocketFrame(JsonUtil.getObjectMapper().writeValueAsString(close)));

            // Close client channel
            ch.close().sync();
        } finally {
            s.shutdown();
            group.shutdownGracefully();
        }
    }
}

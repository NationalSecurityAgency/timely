package timely.server.test;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import timely.common.component.AuthenticationService;
import timely.common.configuration.CorsProperties;
import timely.common.configuration.HttpProperties;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.ServerProperties;
import timely.common.configuration.SslServerProperties;
import timely.common.configuration.TimelyProperties;
import timely.common.configuration.WebsocketProperties;
import timely.server.Server;
import timely.server.store.DataStore;
import timely.server.store.MetaCache;
import timely.server.store.cache.DataStoreCache;

public class TestServer extends Server {

    private static final Logger log = LoggerFactory.getLogger(TestServer.class);
    public TestCaptureRequestHandler httpRequests;
    public TestCaptureRequestHandler tcpRequests;
    public TestCaptureRequestHandler udpRequests;

    public TestServer(ApplicationContext applicationContext, AccumuloClient accumuloClient, DataStore dataStore, DataStoreCache dataStoreCache,
                    @Qualifier("nettySslContext") SslContext sslContext, AuthenticationService authenticationService, CuratorFramework curatorFramework,
                    MetaCache metaCache, TimelyProperties timelyProperties, SecurityProperties securityProperties, ServerProperties serverProperties,
                    HttpProperties httpProperties, CorsProperties corsProperties, WebsocketProperties websocketProperties,
                    SslServerProperties sslServerProperties, @Qualifier("http") TestCaptureRequestHandler httpRequests,
                    @Qualifier("tcp") TestCaptureRequestHandler tcpRequests, @Qualifier("udp") TestCaptureRequestHandler udpRequests) {
        super(applicationContext, accumuloClient, dataStore, dataStoreCache, sslContext, authenticationService, curatorFramework, metaCache, timelyProperties,
                        securityProperties, serverProperties, httpProperties, corsProperties, websocketProperties, sslServerProperties);
        DEFAULT_EVENT_LOOP_THREADS = 1;
        this.httpRequests = httpRequests;
        this.tcpRequests = tcpRequests;
        this.udpRequests = udpRequests;
    }

    @Override
    protected ChannelHandler setupHttpChannelHandler(MetaCache metaCache, TimelyProperties timelyProperties, HttpProperties httpProperties, SslContext sslCtx) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                TestServer.super.setupHttpSocketChannel(ch, metaCache, timelyProperties, httpProperties, sslCtx);
                ch.pipeline().addBefore("put", "capture", httpRequests);
            }
        };
    }

    @Override
    protected ChannelHandler setupTcpChannel() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                TestServer.super.setupTcpSocketChannel(ch);
                ch.pipeline().addBefore("putHandler", "capture", tcpRequests);
            }
        };
    }

    @Override
    protected ChannelHandler setupUdpChannelHandler() {
        return new ChannelInitializer<DatagramChannel>() {

            @Override
            protected void initChannel(DatagramChannel ch) {
                TestServer.super.setupUdpSocketChannel(ch);
                ch.pipeline().addBefore("putHandler", "capture", udpRequests);
            }
        };
    }

    @Override
    protected Channel bind(AbstractBootstrap server, String ip, int port) {
        Channel channel = null;
        long start = System.currentTimeMillis();
        long now = start;
        int attempts = 0;
        while (channel == null && ((now - start) < 30000) && attempts < 10) {
            try {
                log.trace("Binding to port:" + ip + ":" + port + " attempt " + ++attempts);
                channel = server.bind(ip, port).sync().channel();
                now = System.currentTimeMillis();
                if (channel == null) {
                    Thread.sleep(1000);
                }
            } catch (Throwable t) {
                log.error(t.getMessage() + " Binding to port:" + ip + ":" + port);
            }
        }
        if (channel == null) {
            throw new IllegalStateException("Failed to bind to port:" + ip + ":" + port);
        }
        log.trace("Successfully bound to port:" + ip + ":" + port + " in " + attempts + " attempts (" + (now - start) + "ms)");
        return channel;
    }

    @Override
    protected void ensureZkPaths(CuratorFramework curatorFramework, String[] paths) {

    }

    @Override
    protected void registerService(CuratorFramework curatorFramework) {

    }
}

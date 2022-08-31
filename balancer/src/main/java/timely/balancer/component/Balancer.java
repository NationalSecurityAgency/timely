package timely.balancer.component;

import static org.apache.accumulo.core.conf.ConfigurationTypeHelper.getTimeInMillis;
import static timely.Constants.NON_CACHED_METRICS;
import static timely.Constants.NON_CACHED_METRICS_LOCK_PATH;
import static timely.Constants.SERVICE_DISCOVERY_PATH;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.net.ssl.SSLContext;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import io.netty.bootstrap.AbstractBootstrap;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollChannelOption;
import io.netty.channel.epoll.EpollDatagramChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSslServerContext;
import io.netty.handler.ssl.OpenSslServerSessionContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.SystemPropertyUtil;
import timely.auth.JWTTokenHandler;
import timely.balancer.configuration.BalancerHttpProperties;
import timely.balancer.configuration.BalancerProperties;
import timely.balancer.configuration.BalancerServerProperties;
import timely.balancer.configuration.BalancerWebsocketProperties;
import timely.balancer.connection.http.HttpClientPool;
import timely.balancer.connection.tcp.TcpClientPool;
import timely.balancer.connection.udp.UdpClientPool;
import timely.balancer.connection.ws.WsClientPool;
import timely.balancer.netty.http.HttpRelayHandler;
import timely.balancer.netty.tcp.TcpRelayHandler;
import timely.balancer.netty.udp.UdpRelayHandler;
import timely.balancer.netty.ws.WsRelayHandler;
import timely.client.http.HttpClient;
import timely.common.component.AuthenticationService;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.SslClientProperties;
import timely.common.configuration.SslHelper;
import timely.common.configuration.SslServerProperties;
import timely.common.configuration.ZookeeperProperties;
import timely.netty.http.HttpStaticFileServerHandler;
import timely.netty.http.NonSslRedirectHandler;
import timely.netty.http.TimelyExceptionHandler;
import timely.netty.http.auth.X509LoginRequestHandler;
import timely.netty.tcp.MetricsBufferDecoder;
import timely.netty.tcp.TcpDecoder;
import timely.netty.tcp.TcpVersionHandler;
import timely.netty.udp.UdpDecoder;
import timely.netty.udp.UdpPacketToByteBuf;
import timely.netty.websocket.WebSocketFullRequestHandler;
import timely.netty.websocket.WebSocketRequestDecoder;
import timely.netty.websocket.subscription.WSTimelyExceptionHandler;

@Component
@ConditionalOnMissingBean(Balancer.class)
public class Balancer {

    final public static String LEADER_LATCH_PATH = "/timely/balancer/leader";
    final public static String ASSIGNMENTS_LOCK_PATH = "/timely/balancer/assignments/lock";
    final public static String ASSIGNMENTS_LAST_UPDATED_PATH = "/timely/balancer/assignments/lastUpdated";

    private static final Logger log = LoggerFactory.getLogger(Balancer.class);
    private static final String EPOLL_MIN_VERSION = "2.7";
    private static final String OS_NAME = "os.name";
    private static final String OS_VERSION = "os.version";
    private static final String WS_PATH = "/websocket";
    protected static final CountDownLatch LATCH = new CountDownLatch(1);

    private BalancerProperties balancerProperties;
    private SecurityProperties securityProperties;
    private SslServerProperties sslServerProperties;
    private SslClientProperties sslClientProperties;
    private ZookeeperProperties zookeeperProperties;
    private BalancerServerProperties balancerServerProperties;
    private BalancerHttpProperties balancerHttpProperties;
    private BalancerWebsocketProperties balancerWebsocketProperties;
    private ApplicationContext applicationContext;
    private AuthenticationService authenticationService;
    private CuratorFramework curatorFramework;
    private int quietPeriod;

    private EventLoopGroup tcpWorkerGroup = null;
    private EventLoopGroup tcpBossGroup = null;
    private EventLoopGroup httpWorkerGroup = null;
    private EventLoopGroup httpBossGroup = null;
    private EventLoopGroup wsWorkerGroup = null;
    private EventLoopGroup wsBossGroup = null;
    private EventLoopGroup udpBossGroup = null;
    private EventLoopGroup udpWorkerGroup = null;
    private MetricResolver metricResolver;
    protected Channel tcpChannelHandle = null;
    protected Channel httpChannelHandle = null;
    protected Channel wsChannelHandle = null;
    protected List<Channel> udpChannelHandleList = new ArrayList<>();
    protected int DEFAULT_EVENT_LOOP_THREADS;

    private String[] zkPaths = new String[] {LEADER_LATCH_PATH, ASSIGNMENTS_LAST_UPDATED_PATH, ASSIGNMENTS_LOCK_PATH, SERVICE_DISCOVERY_PATH,
            NON_CACHED_METRICS, NON_CACHED_METRICS_LOCK_PATH};

    private static boolean useEpoll() {
        final String osName = SystemPropertyUtil.get(OS_NAME).toLowerCase().trim();
        final String osVersion = SystemPropertyUtil.get(OS_VERSION).toLowerCase();
        // split at periods, keep the first two, join with a period
        final String majMinVers = StringUtils.join(".", Arrays.stream(osVersion.split("\\.")).limit(2).collect(Collectors.toList()));
        if (osName.startsWith("linux")) {
            Runtime.Version currentVersion = Runtime.Version.parse(majMinVers);
            Runtime.Version epollMinVersion = Runtime.Version.parse(EPOLL_MIN_VERSION);
            return currentVersion.compareTo(epollMinVersion) > 0;
        } else {
            return false;
        }
    }

    public Balancer(ApplicationContext applicationContext, AuthenticationService authenticationService, MetricResolver metricResolver,
                    BalancerProperties balancerProperties, ZookeeperProperties zookeeperProperties, SecurityProperties securityProperties,
                    SslServerProperties sslServerProperties, SslClientProperties sslClientProperties, BalancerServerProperties balancerServerProperties,
                    BalancerHttpProperties balancerHttpProperties, BalancerWebsocketProperties balancerWebsocketProperties) {
        this.quietPeriod = balancerServerProperties.getShutdownQuietPeriod();
        this.applicationContext = applicationContext;
        this.authenticationService = authenticationService;
        this.metricResolver = metricResolver;
        this.balancerProperties = balancerProperties;
        this.zookeeperProperties = zookeeperProperties;
        this.securityProperties = securityProperties;
        this.sslServerProperties = sslServerProperties;
        this.sslClientProperties = sslClientProperties;
        this.balancerServerProperties = balancerServerProperties;
        this.balancerHttpProperties = balancerHttpProperties;
        this.balancerWebsocketProperties = balancerWebsocketProperties;
        DEFAULT_EVENT_LOOP_THREADS = Math.max(1, SystemPropertyUtil.getInt("io.netty.eventLoopThreads", NettyRuntime.availableProcessors() * 2));
    }

    @PostConstruct
    public void startup() {
        log.info("Starting {}", this.getClass().getSimpleName());
        try {
            // start curator framework
            RetryPolicy retryPolicy = new RetryForever(1000);
            int timeout = Long.valueOf(getTimeInMillis(zookeeperProperties.getTimeout())).intValue();
            curatorFramework = CuratorFrameworkFactory.newClient(zookeeperProperties.getServers(), timeout, 10000, retryPolicy);
            curatorFramework.start();
            ensureZkPaths(curatorFramework, zkPaths);

            JWTTokenHandler.init(securityProperties, null);
            final boolean useEpoll = useEpoll();
            Class<? extends ServerSocketChannel> channelClass;
            Class<? extends Channel> datagramChannelClass;
            if (useEpoll) {
                tcpWorkerGroup = new EpollEventLoopGroup();
                tcpBossGroup = new EpollEventLoopGroup();
                httpWorkerGroup = new EpollEventLoopGroup();
                httpBossGroup = new EpollEventLoopGroup();
                wsWorkerGroup = new EpollEventLoopGroup();
                wsBossGroup = new EpollEventLoopGroup();
                udpWorkerGroup = new EpollEventLoopGroup();
                udpBossGroup = new EpollEventLoopGroup();
                channelClass = EpollServerSocketChannel.class;
                datagramChannelClass = EpollDatagramChannel.class;
            } else {
                tcpWorkerGroup = new NioEventLoopGroup();
                tcpBossGroup = new NioEventLoopGroup();
                httpWorkerGroup = new NioEventLoopGroup();
                httpBossGroup = new NioEventLoopGroup();
                wsWorkerGroup = new NioEventLoopGroup();
                wsBossGroup = new NioEventLoopGroup();
                udpWorkerGroup = new NioEventLoopGroup();
                udpBossGroup = new NioEventLoopGroup();
                channelClass = NioServerSocketChannel.class;
                datagramChannelClass = NioDatagramChannel.class;
            }
            log.info("Using channel class {}", channelClass.getSimpleName());

            List<TcpClientPool> tcpClientPools = new ArrayList<>();
            for (int x = 0; x < balancerServerProperties.getNumTcpPools(); x++) {
                tcpClientPools.add(new TcpClientPool(balancerServerProperties));
            }

            final ServerBootstrap tcpServer = new ServerBootstrap();
            tcpServer.group(tcpBossGroup, tcpWorkerGroup);
            tcpServer.channel(channelClass);
            tcpServer.handler(new LoggingHandler());
            tcpServer.childHandler(setupTcpChannel(metricResolver, tcpClientPools));
            tcpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            tcpServer.option(ChannelOption.SO_BACKLOG, 128);
            tcpServer.childOption(ChannelOption.SO_KEEPALIVE, true);
            final int tcpPort = balancerServerProperties.getTcpPort();
            final String tcpIp = balancerServerProperties.getIp();
            tcpChannelHandle = bind(tcpServer, tcpIp, tcpPort);
            final String tcpAddress = ((InetSocketAddress) tcpChannelHandle.localAddress()).getAddress().getHostAddress();

            final int httpPort = balancerHttpProperties.getPort();
            final String httpIp = balancerHttpProperties.getIp();
            SslContext sslCtx = createSSLContext(sslServerProperties);
            if (sslCtx instanceof OpenSslServerContext) {
                OpenSslServerContext openssl = (OpenSslServerContext) sslCtx;
                String application = "Timely_" + httpPort;
                OpenSslServerSessionContext opensslCtx = openssl.sessionContext();
                opensslCtx.setSessionCacheEnabled(true);
                opensslCtx.setSessionCacheSize(128);
                opensslCtx.setSessionIdContext(application.getBytes(StandardCharsets.UTF_8));
                opensslCtx.setSessionTimeout(securityProperties.getSessionMaxAge());
            }
            SSLContext clientSSLContext = createSSLClientContext(sslClientProperties);
            final ServerBootstrap httpServer = new ServerBootstrap();
            httpServer.group(httpBossGroup, httpWorkerGroup);
            httpServer.channel(channelClass);
            httpServer.handler(new LoggingHandler());
            HttpClientPool httpClientPool = new HttpClientPool(securityProperties, sslClientProperties, balancerHttpProperties, clientSSLContext);
            httpServer.childHandler(setupHttpChannel(balancerHttpProperties, sslServerProperties, sslCtx, metricResolver, httpClientPool));
            httpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            httpServer.option(ChannelOption.SO_BACKLOG, 128);
            httpServer.childOption(ChannelOption.SO_KEEPALIVE, true);
            httpChannelHandle = bind(httpServer, httpIp, httpPort);
            final String httpAddress = ((InetSocketAddress) httpChannelHandle.localAddress()).getAddress().getHostAddress();

            final int wsPort = balancerWebsocketProperties.getPort();
            final String wsIp = balancerWebsocketProperties.getIp();
            final ServerBootstrap wsServer = new ServerBootstrap();
            wsServer.group(wsBossGroup, wsWorkerGroup);
            wsServer.channel(channelClass);
            wsServer.handler(new LoggingHandler());
            WsClientPool wsClientPool = new WsClientPool(balancerProperties, balancerWebsocketProperties, sslClientProperties, clientSSLContext);
            wsServer.childHandler(setupWSChannel(balancerWebsocketProperties, securityProperties, sslCtx, metricResolver, wsClientPool));
            wsServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            wsServer.option(ChannelOption.SO_BACKLOG, 128);
            wsServer.childOption(ChannelOption.SO_KEEPALIVE, true);
            wsChannelHandle = bind(wsServer, wsIp, wsPort);
            final String wsAddress = ((InetSocketAddress) wsChannelHandle.localAddress()).getAddress().getHostAddress();

            final int udpPort = balancerServerProperties.getUdpPort();
            final String udpIp = balancerServerProperties.getIp();
            UdpClientPool udpClientPool = new UdpClientPool(balancerServerProperties);
            for (int n = 0; n < DEFAULT_EVENT_LOOP_THREADS; n++) {
                final Bootstrap udpServer = new Bootstrap();
                udpServer.group(udpBossGroup);
                udpServer.channel(datagramChannelClass);
                udpServer.handler(setupUdpChannel(metricResolver, udpClientPool));
                udpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
                udpServer.option(EpollChannelOption.SO_REUSEADDR, true);
                udpServer.option(EpollChannelOption.SO_REUSEPORT, true);
                udpChannelHandleList.add(bind(udpServer, udpIp, udpPort));
            }
            log.info("Balancer started. Listening on {}:{} for TCP traffic, {}:{} for HTTP traffic, {}:{} for WebSocket traffic, and {}:{} for UDP traffic",
                            tcpAddress, tcpPort, httpAddress, httpPort, wsAddress, wsPort, wsAddress, udpPort);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            SpringApplication.exit(applicationContext, () -> 0);
        }
    }

    protected Channel bind(AbstractBootstrap server, String ip, int port) {
        Channel channel = null;
        try {
            channel = server.bind(ip, port).sync().channel();
        } catch (InterruptedException e) {
            log.error("Failed to bind to {}:{} -- {}", ip, port, e.getMessage());
        }
        if (channel == null) {
            throw new IllegalStateException("Failed to bind to port:" + ip + ":" + port);
        }
        return channel;
    }

    @PreDestroy
    public void shutdown() {
        List<ChannelFuture> channelFutures = new ArrayList<>();

        if (tcpChannelHandle != null) {
            log.info("Closing tcpChannelHandle");
            channelFutures.add(tcpChannelHandle.close());
        }

        if (httpChannelHandle != null) {
            log.info("Closing httpChannelHandle");
            channelFutures.add(httpChannelHandle.close());
        }

        if (wsChannelHandle != null) {
            log.info("Closing wsChannelHandle");
            channelFutures.add(wsChannelHandle.close());
        }

        int udpChannel = 1;
        for (Channel c : udpChannelHandleList) {
            log.info("Closing udpChannelHandle #" + udpChannel++);
            channelFutures.add(c.close());
        }

        // wait for the channels to shutdown
        channelFutures.forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                log.error("Channel:" + f.channel().config() + " -> " + e.getMessage(), e);
            }
        });

        List<Future<?>> groupFutures = new ArrayList<>();

        if (tcpBossGroup != null) {
            log.info("Shutting down tcpBossGroup");
            groupFutures.add(tcpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (tcpWorkerGroup != null) {
            log.info("Shutting down tcpWorkerGroup");
            groupFutures.add(tcpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (httpBossGroup != null) {
            log.info("Shutting down httpBossGroup");
            groupFutures.add(httpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (httpWorkerGroup != null) {
            log.info("Shutting down httpWorkerGroup");
            groupFutures.add(httpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (wsBossGroup != null) {
            log.info("Shutting down wsBossGroup");
            groupFutures.add(wsBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (wsWorkerGroup != null) {
            log.info("Shutting down wsWorkerGroup");
            groupFutures.add(wsWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (udpBossGroup != null) {
            log.info("Shutting down udpBossGroup");
            groupFutures.add(udpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (udpWorkerGroup != null) {
            log.info("Shutting down udpWorkerGroup");
            groupFutures.add(udpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        groupFutures.parallelStream().forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                log.error("Group:" + f.toString() + " -> " + e.getMessage(), e);
            }
        });

        try {
            log.info("Closing WebSocketRequestDecoder");
            WebSocketRequestDecoder.close();
        } catch (Exception e) {
            log.error("Error closing WebSocketRequestDecoder during shutdown", e);
        }
        log.info("{} shut down.", this.getClass().getSimpleName());
    }

    protected SslContext createSSLContext(SslServerProperties sslServerProperties) throws Exception {

        Boolean generate = sslServerProperties.isUseGeneratedKeypair();
        SslContextBuilder ssl;
        if (generate) {
            log.warn("Using generated self signed server certificate");
            Date begin = new Date();
            Date end = new Date(begin.getTime() + 86400000);
            SelfSignedCertificate ssc = new SelfSignedCertificate("localhost", begin, end);
            ssl = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
        } else {
            ssl = SslHelper.getSslContextBuilder(sslServerProperties);
        }
        // Can't set to REQUIRE because the CORS pre-flight requests will fail.
        ssl.clientAuth(ClientAuth.OPTIONAL);
        return ssl.build();
    }

    protected SSLContext createSSLClientContext(SslClientProperties sslClientProperties) {

        SslClientProperties ssl = sslClientProperties;
        return HttpClient.getSSLContext(ssl.getTrustStoreFile(), ssl.getTrustStoreType(), ssl.getTrustStorePassword(), ssl.getKeyStoreFile(),
                        ssl.getKeyStoreType(), ssl.getKeyStorePassword());
    }

    private void ensureZkPaths(CuratorFramework curatorFramework, String[] paths) {
        for (String s : paths) {
            try {
                Stat stat = curatorFramework.checkExists().forPath(s);
                if (stat == null) {
                    curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT).forPath(s);
                }
            } catch (Exception e) {
                log.info(e.getMessage());
            }

        }
    }

    protected ChannelHandler setupHttpChannel(BalancerHttpProperties balancerHttpProperties, SslServerProperties sslServerProperties, SslContext sslCtx,
                    MetricResolver metricResolver, HttpClientPool httpClientPool) {

        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {

                ch.pipeline().addLast("ssl", new NonSslRedirectHandler(balancerHttpProperties, sslCtx));
                ch.pipeline().addLast("encoder", new HttpResponseEncoder());
                ch.pipeline().addLast("decoder", new HttpRequestDecoder(4096, 32768, 8192, true, 128));
                ch.pipeline().addLast("compressor", new HttpContentCompressor());
                ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
                ch.pipeline().addLast("chunker", new ChunkedWriteHandler());
                ch.pipeline().addLast("queryDecoder",
                                new timely.netty.http.HttpRequestDecoder(authenticationService, securityProperties, balancerHttpProperties));
                ch.pipeline().addLast("fileServer",
                                new HttpStaticFileServerHandler().setIgnoreSslHandshakeErrors(sslServerProperties.isIgnoreSslHandshakeErrors()));
                ch.pipeline().addLast("login", new X509LoginRequestHandler(authenticationService, securityProperties, balancerHttpProperties));
                ch.pipeline().addLast("httpRelay", new HttpRelayHandler(metricResolver, httpClientPool));
                ch.pipeline().addLast("error", new TimelyExceptionHandler().setIgnoreSslHandshakeErrors(sslServerProperties.isIgnoreSslHandshakeErrors()));
            }
        };
    }

    protected ChannelHandler setupUdpChannel(MetricResolver metricResolver, UdpClientPool udpClientPool) {
        return new ChannelInitializer<DatagramChannel>() {

            @Override
            protected void initChannel(DatagramChannel ch) throws Exception {
                ch.pipeline().addLast("logger", new LoggingHandler());
                ch.pipeline().addLast("packetDecoder", new UdpPacketToByteBuf());
                ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(65536, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new UdpDecoder());
                ch.pipeline().addLast("udpRelayHandler", new UdpRelayHandler(metricResolver, udpClientPool));
            }
        };
    }

    protected ChannelHandler setupTcpChannel(MetricResolver metricResolver, List<TcpClientPool> tcpClientPools) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(65536, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new TcpDecoder());
                ch.pipeline().addLast("tcpRelayHandler", new TcpRelayHandler(metricResolver, tcpClientPools));
                ch.pipeline().addLast("versionHandler", new TcpVersionHandler());
            }
        };
    }

    protected ChannelHandler setupWSChannel(BalancerWebsocketProperties balancerWebsocketProperties, SecurityProperties securityProperties, SslContext sslCtx,
                    MetricResolver metricResolver, WsClientPool wsClientPool) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("ssl", sslCtx.newHandler(ch.alloc()));
                ch.pipeline().addLast("httpServer", new HttpServerCodec());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
                ch.pipeline().addLast("sessionExtractor", new WebSocketFullRequestHandler(authenticationService));

                ch.pipeline().addLast("idle-handler", new IdleStateHandler(balancerWebsocketProperties.getTimeout(), 0, 0));
                ch.pipeline().addLast("ws-protocol", new WebSocketServerProtocolHandler(WS_PATH, null, true, 65536, false, true));
                ch.pipeline().addLast("wsDecoder", new WebSocketRequestDecoder(authenticationService, securityProperties));
                ch.pipeline().addLast("httpRelay", new WsRelayHandler(balancerWebsocketProperties, metricResolver, wsClientPool));
                ch.pipeline().addLast("error", new WSTimelyExceptionHandler());
            }
        };
    }
}

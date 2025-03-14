package timely.server;

import static timely.Constants.NON_CACHED_METRICS;
import static timely.Constants.NON_CACHED_METRICS_LOCK_PATH;
import static timely.Constants.SERVICE_DISCOVERY_PATH;

import java.lang.Runtime.Version;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;

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
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.OpenSslServerContext;
import io.netty.handler.ssl.OpenSslServerSessionContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.SystemPropertyUtil;
import timely.ServerDetails;
import timely.auth.JWTTokenHandler;
import timely.common.component.AuthenticationService;
import timely.common.configuration.CorsProperties;
import timely.common.configuration.HttpProperties;
import timely.common.configuration.SecurityProperties;
import timely.common.configuration.ServerProperties;
import timely.common.configuration.SslServerProperties;
import timely.common.configuration.TimelyProperties;
import timely.common.configuration.WebsocketProperties;
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
import timely.server.netty.http.HttpCacheRequestHandler;
import timely.server.netty.http.HttpMetricPutHandler;
import timely.server.netty.http.HttpVersionRequestHandler;
import timely.server.netty.http.StrictTransportHandler;
import timely.server.netty.http.timeseries.HttpAggregatorsRequestHandler;
import timely.server.netty.http.timeseries.HttpMetricsRequestHandler;
import timely.server.netty.http.timeseries.HttpQueryRequestHandler;
import timely.server.netty.http.timeseries.HttpSearchLookupRequestHandler;
import timely.server.netty.http.timeseries.HttpSuggestRequestHandler;
import timely.server.netty.tcp.TcpPutHandler;
import timely.server.netty.websocket.WSMetricPutHandler;
import timely.server.netty.websocket.WSVersionRequestHandler;
import timely.server.netty.websocket.subscription.WSAddSubscriptionRequestHandler;
import timely.server.netty.websocket.subscription.WSCloseSubscriptionRequestHandler;
import timely.server.netty.websocket.subscription.WSCreateSubscriptionRequestHandler;
import timely.server.netty.websocket.subscription.WSRemoveSubscriptionRequestHandler;
import timely.server.netty.websocket.timeseries.WSAggregatorsRequestHandler;
import timely.server.netty.websocket.timeseries.WSMetricsRequestHandler;
import timely.server.netty.websocket.timeseries.WSQueryRequestHandler;
import timely.server.netty.websocket.timeseries.WSSearchLookupRequestHandler;
import timely.server.netty.websocket.timeseries.WSSuggestRequestHandler;
import timely.server.store.DataStore;
import timely.server.store.MetaCache;
import timely.server.store.cache.DataStoreCache;

public class Server {

    final public static String LEADER_LATCH_PATH = "/timely/server/leader";

    private static final Logger log = LoggerFactory.getLogger(Server.class);
    private static final String EPOLL_MIN_VERSION = "2.7";
    private static final String OS_NAME = "os.name";
    private static final String OS_VERSION = "os.version";
    private static final String WS_PATH = "/websocket";

    private AccumuloClient accumuloClient;
    private TimelyProperties timelyProperties;
    private SecurityProperties securityProperties;
    private ServerProperties serverProperties;
    private HttpProperties httpProperties;
    private CorsProperties corsProperties;
    private WebsocketProperties websocketProperties;
    private SslServerProperties sslServerProperties;

    protected AuthenticationService authenticationService;
    private CuratorFramework curatorFramework;
    protected MetaCache metaCache;
    protected DataStore dataStore;
    protected DataStoreCache dataStoreCache;
    protected ApplicationContext applicationContext;

    private int shutdownQuietPeriod;
    private EventLoopGroup tcpWorkerGroup = null;
    private EventLoopGroup tcpBossGroup = null;
    private EventLoopGroup httpWorkerGroup = null;
    private EventLoopGroup httpBossGroup = null;
    private EventLoopGroup wsWorkerGroup = null;
    private EventLoopGroup wsBossGroup = null;
    private EventLoopGroup udpBossGroup = null;
    private EventLoopGroup udpWorkerGroup = null;
    protected Channel tcpChannelHandle = null;
    protected Channel httpChannelHandle = null;
    protected Channel wsChannelHandle = null;
    protected List<Channel> udpChannelHandleList = new ArrayList<>();
    private LeaderLatch leaderLatch;
    private SslContext sslContext;
    protected int DEFAULT_EVENT_LOOP_THREADS;

    private String[] zkPaths = new String[] {SERVICE_DISCOVERY_PATH, LEADER_LATCH_PATH, NON_CACHED_METRICS, NON_CACHED_METRICS_LOCK_PATH};

    private static boolean useEpoll() {
        final String osName = SystemPropertyUtil.get(OS_NAME).toLowerCase().trim();
        final String osVersion = SystemPropertyUtil.get(OS_VERSION).toLowerCase();
        // split at periods, keep the first two, join with a period
        final String majMinVers = StringUtils.join(Arrays.stream(osVersion.split("\\.")).limit(2).collect(Collectors.toList()), ".");
        if (osName.startsWith("linux")) {
            try {
                Version currentVersion = Version.parse(majMinVers);
                Version epollMinVersion = Version.parse(EPOLL_MIN_VERSION);
                return currentVersion.compareTo(epollMinVersion) > 0;
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        return false;
    }

    public Server(ApplicationContext applicationContext, AccumuloClient accumuloClient, DataStore dataStore, DataStoreCache dataStoreCache,
                    @Qualifier("nettySslContext") SslContext sslContext, AuthenticationService authenticationService, CuratorFramework curatorFramework,
                    MetaCache metaCache, TimelyProperties timelyProperties, SecurityProperties securityProperties, ServerProperties serverProperties,
                    HttpProperties httpProperties, CorsProperties corsProperties, WebsocketProperties websocketProperties,
                    SslServerProperties sslServerProperties) {

        this.accumuloClient = accumuloClient;
        this.timelyProperties = timelyProperties;
        this.securityProperties = securityProperties;
        this.serverProperties = serverProperties;
        this.httpProperties = httpProperties;
        this.corsProperties = corsProperties;
        this.websocketProperties = websocketProperties;
        this.sslServerProperties = sslServerProperties;
        this.curatorFramework = curatorFramework;
        this.metaCache = metaCache;
        this.applicationContext = applicationContext;
        this.dataStore = dataStore;
        this.dataStoreCache = dataStoreCache;
        this.sslContext = sslContext;
        this.authenticationService = authenticationService;
        this.DEFAULT_EVENT_LOOP_THREADS = Math.max(1, SystemPropertyUtil.getInt("io.netty.eventLoopThreads", NettyRuntime.availableProcessors() * 2));
    }

    public void start() {
        log.info("Starting {}", this.getClass().getSimpleName());
        try {
            shutdownQuietPeriod = serverProperties.getShutdownQuietPeriod();
            ensureZkPaths(curatorFramework, zkPaths);
            JWTTokenHandler.init(securityProperties, accumuloClient);
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

            log.info("Creating tcp server");
            final ServerBootstrap tcpServer = new ServerBootstrap();
            tcpServer.group(this.tcpBossGroup, this.tcpWorkerGroup);
            tcpServer.channel(channelClass);
            tcpServer.handler(new LoggingHandler());
            tcpServer.childHandler(setupTcpChannel());
            tcpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            tcpServer.option(ChannelOption.SO_BACKLOG, 128);
            tcpServer.childOption(ChannelOption.SO_KEEPALIVE, true);
            final int tcpPort = serverProperties.getTcpPort();
            final String tcpIp = serverProperties.getIp();
            this.tcpChannelHandle = bind(tcpServer, tcpIp, tcpPort);
            final String tcpAddress = ((InetSocketAddress) this.tcpChannelHandle.localAddress()).getAddress().getHostAddress();

            log.info("Creating http server");
            final int httpPort = httpProperties.getPort();
            final String httpIp = httpProperties.getIp();
            if (this.sslContext instanceof OpenSslServerContext) {
                OpenSslServerContext openssl = (OpenSslServerContext) this.sslContext;
                String application = "Timely_" + httpPort;
                OpenSslServerSessionContext opensslCtx = openssl.sessionContext();
                opensslCtx.setSessionCacheEnabled(true);
                opensslCtx.setSessionCacheSize(128);
                opensslCtx.setSessionIdContext(application.getBytes(StandardCharsets.UTF_8));
                opensslCtx.setSessionTimeout(securityProperties.getSessionMaxAge());
            }
            final ServerBootstrap httpServer = new ServerBootstrap();
            httpServer.group(httpBossGroup, httpWorkerGroup);
            httpServer.channel(channelClass);
            httpServer.handler(new LoggingHandler());
            httpServer.childHandler(setupHttpChannelHandler(metaCache, timelyProperties, httpProperties, sslContext));
            httpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            httpServer.option(ChannelOption.SO_BACKLOG, 128);
            httpServer.option(ChannelOption.SO_BACKLOG, 128);
            httpServer.childOption(ChannelOption.SO_KEEPALIVE, true);
            httpChannelHandle = bind(httpServer, httpIp, httpPort);
            final String httpAddress = ((InetSocketAddress) httpChannelHandle.localAddress()).getAddress().getHostAddress();

            log.info("Creating ws server");
            final int wsPort = websocketProperties.getPort();
            final String wsIp = websocketProperties.getIp();
            final ServerBootstrap wsServer = new ServerBootstrap();
            wsServer.group(wsBossGroup, wsWorkerGroup);
            wsServer.channel(channelClass);
            wsServer.handler(new LoggingHandler());
            wsServer.childHandler(setupWSChannel(metaCache, timelyProperties, websocketProperties, sslContext));
            wsServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            wsServer.option(ChannelOption.SO_BACKLOG, 128);
            wsServer.childOption(ChannelOption.SO_KEEPALIVE, true);
            wsChannelHandle = bind(wsServer, wsIp, wsPort);
            final String wsAddress = ((InetSocketAddress) wsChannelHandle.localAddress()).getAddress().getHostAddress();

            log.info("Creating udp server");
            final int udpPort = serverProperties.getUdpPort();
            final String udpIp = serverProperties.getIp();
            for (int n = 0; n < DEFAULT_EVENT_LOOP_THREADS; n++) {
                final Bootstrap udpServer = new Bootstrap();
                udpServer.group(udpBossGroup);
                udpServer.channel(datagramChannelClass);
                udpServer.handler(setupUdpChannelHandler());
                udpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
                udpServer.option(EpollChannelOption.SO_REUSEADDR, true);
                udpServer.option(EpollChannelOption.SO_REUSEPORT, true);
                udpChannelHandleList.add(bind(udpServer, udpIp, udpPort));
            }
            registerService(curatorFramework);
            log.info("TimelyServer started. Listening on {}:{} for TCP traffic, {}:{} for HTTP traffic, {}:{} for WebSocket traffic, and {}:{} for UDP traffic",
                            tcpAddress, tcpPort, httpAddress, httpPort, wsAddress, wsPort, wsAddress, udpPort);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            SpringApplication.exit(applicationContext, () -> 0);
        }
    }

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
            groupFutures.add(tcpBossGroup.shutdownGracefully(shutdownQuietPeriod, 10, TimeUnit.SECONDS));
        }

        if (tcpWorkerGroup != null) {
            log.info("Shutting down tcpWorkerGroup");
            groupFutures.add(tcpWorkerGroup.shutdownGracefully(shutdownQuietPeriod, 10, TimeUnit.SECONDS));
        }

        if (httpBossGroup != null) {
            log.info("Shutting down httpBossGroup");
            groupFutures.add(httpBossGroup.shutdownGracefully(shutdownQuietPeriod, 10, TimeUnit.SECONDS));
        }

        if (httpWorkerGroup != null) {
            log.info("Shutting down httpWorkerGroup");
            groupFutures.add(httpWorkerGroup.shutdownGracefully(shutdownQuietPeriod, 10, TimeUnit.SECONDS));
        }

        if (wsBossGroup != null) {
            log.info("Shutting down wsBossGroup");
            groupFutures.add(wsBossGroup.shutdownGracefully(shutdownQuietPeriod, 10, TimeUnit.SECONDS));
        }

        if (wsWorkerGroup != null) {
            log.info("Shutting down wsWorkerGroup");
            groupFutures.add(wsWorkerGroup.shutdownGracefully(shutdownQuietPeriod, 10, TimeUnit.SECONDS));
        }

        if (udpBossGroup != null) {
            log.info("Shutting down udpBossGroup");
            groupFutures.add(udpBossGroup.shutdownGracefully(shutdownQuietPeriod, 10, TimeUnit.SECONDS));
        }

        if (udpWorkerGroup != null) {
            log.info("Shutting down udpWorkerGroup");
            groupFutures.add(udpWorkerGroup.shutdownGracefully(shutdownQuietPeriod, 10, TimeUnit.SECONDS));
        }

        groupFutures.parallelStream().forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                log.error("Group:" + f.toString() + " -> " + e.getMessage(), e);
            }
        });

        try {
            log.info("Closing webSocketRequestDecoder subscriptions");
            WebSocketRequestDecoder.close();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
        log.info("{} shut down.", this.getClass().getSimpleName());
    }

    protected void registerService(CuratorFramework curatorFramework) {
        try {
            try {
                Stat stat = curatorFramework.checkExists().forPath(SERVICE_DISCOVERY_PATH);
                if (stat == null) {
                    curatorFramework.create().creatingParentContainersIfNeeded().forPath(SERVICE_DISCOVERY_PATH);
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }

            ServerDetails payload = new ServerDetails();
            String host = serverProperties.getIp();
            try {
                InetAddress inetAddr = InetAddress.getByName(host);
                host = inetAddr.getCanonicalHostName();
            } catch (UnknownHostException e) {
                log.error(e.getMessage(), e);
            }
            payload.setHost(host);
            payload.setTcpPort(serverProperties.getTcpPort());
            payload.setHttpPort(httpProperties.getPort());
            payload.setWsPort(websocketProperties.getPort());
            payload.setUdpPort(serverProperties.getUdpPort());

            ServiceInstanceBuilder<ServerDetails> builder = ServiceInstance.builder();
            String serviceName = host + ":" + serverProperties.getTcpPort();
            ServiceInstance<ServerDetails> serviceInstance = builder.id(serviceName).name("timely-server").address(serverProperties.getIp())
                            .port(serverProperties.getTcpPort()).payload(payload).build();

            ServiceDiscovery<ServerDetails> discovery = ServiceDiscoveryBuilder.builder(ServerDetails.class).client(curatorFramework)
                            .basePath(SERVICE_DISCOVERY_PATH).build();
            discovery.start();
            discovery.registerService(serviceInstance);

        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    protected void ensureZkPaths(CuratorFramework curatorFramework, String[] paths) {
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

    public void startLeaderLatch() {
        try {
            this.leaderLatch = new LeaderLatch(this.curatorFramework, LEADER_LATCH_PATH);
            this.leaderLatch.start();
            this.leaderLatch.addListener(new LeaderLatchListener() {

                @Override
                public void isLeader() {
                    try {
                        log.info("This server is the leader, applying ageoff settings to Accumulo");
                        dataStore.applyAgeOffIterators();
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }

                @Override
                public void notLeader() {
                    log.info("This server is not the leader");
                }
            });
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    protected void setupHttpSocketChannel(SocketChannel ch, MetaCache metaCache, TimelyProperties timelyProperties, HttpProperties httpProperties,
                    SslContext sslCtx) {
        ch.pipeline().addLast("ssl", new NonSslRedirectHandler(httpProperties, sslCtx));
        ch.pipeline().addLast("encoder", new HttpResponseEncoder());
        ch.pipeline().addLast("decoder", new HttpRequestDecoder(4096, 32768, 8192, true, 128));
        ch.pipeline().addLast("compressor", new HttpContentCompressor());
        ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
        ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
        ch.pipeline().addLast("chunker", new ChunkedWriteHandler());
        final CorsConfigBuilder ccb;
        if (corsProperties.isAllowAnyOrigin()) {
            ccb = CorsConfigBuilder.forAnyOrigin();
        } else {
            ccb = CorsConfigBuilder.forOrigins(corsProperties.getAllowedOrigins().stream().toArray(String[]::new));
        }
        if (corsProperties.isAllowNullOrigin()) {
            ccb.allowNullOrigin();
        }
        if (corsProperties.isAllowCredentials()) {
            ccb.allowCredentials();
        }
        corsProperties.getAllowedMethods().stream().map(HttpMethod::valueOf).forEach(ccb::allowedRequestMethods);
        corsProperties.getAllowedHeaders().forEach(ccb::allowedRequestHeaders);
        CorsConfig cors = ccb.build();
        log.trace("Cors configuration: {}", cors);
        ch.pipeline().addLast("cors", new CorsHandler(cors));
        ch.pipeline().addLast("queryDecoder", new timely.netty.http.HttpRequestDecoder(authenticationService, securityProperties, httpProperties));
        ch.pipeline().addLast("fileServer", new HttpStaticFileServerHandler().setIgnoreSslHandshakeErrors(sslServerProperties.isIgnoreSslHandshakeErrors()));
        ch.pipeline().addLast("strict", new StrictTransportHandler(httpProperties));
        ch.pipeline().addLast("login", new X509LoginRequestHandler(authenticationService, securityProperties, httpProperties));
        ch.pipeline().addLast("aggregators", new HttpAggregatorsRequestHandler());
        ch.pipeline().addLast("metrics", new HttpMetricsRequestHandler(metaCache, timelyProperties));
        ch.pipeline().addLast("query", new HttpQueryRequestHandler(dataStore));
        ch.pipeline().addLast("search", new HttpSearchLookupRequestHandler(dataStore));
        ch.pipeline().addLast("suggest", new HttpSuggestRequestHandler(dataStore));
        ch.pipeline().addLast("version", new HttpVersionRequestHandler());
        ch.pipeline().addLast("cache", new HttpCacheRequestHandler(dataStoreCache));
        ch.pipeline().addLast("put", new HttpMetricPutHandler(dataStore));
        ch.pipeline().addLast("error", new TimelyExceptionHandler().setIgnoreSslHandshakeErrors(sslServerProperties.isIgnoreSslHandshakeErrors()));
    }

    protected ChannelHandler setupHttpChannelHandler(MetaCache metaCache, TimelyProperties timelyProperties, HttpProperties httpProperties, SslContext sslCtx) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                setupHttpSocketChannel(ch, metaCache, timelyProperties, httpProperties, sslCtx);
            }
        };
    }

    protected void setupUdpSocketChannel(DatagramChannel ch) {
        ch.pipeline().addLast("logger", new LoggingHandler());
        ch.pipeline().addLast("packetDecoder", new UdpPacketToByteBuf());
        ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
        ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(65536, true, Delimiters.lineDelimiter()));
        ch.pipeline().addLast("putDecoder", new UdpDecoder());
        ch.pipeline().addLast("putHandler", new TcpPutHandler(dataStore));
    }

    protected ChannelHandler setupUdpChannelHandler() {
        return new ChannelInitializer<DatagramChannel>() {

            @Override
            protected void initChannel(DatagramChannel ch) {
                setupUdpSocketChannel(ch);
            }
        };
    }

    protected void setupTcpSocketChannel(SocketChannel ch) {
        ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
        ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(65536, true, Delimiters.lineDelimiter()));
        ch.pipeline().addLast("putDecoder", new TcpDecoder());
        ch.pipeline().addLast("putHandler", new TcpPutHandler(dataStore));
        ch.pipeline().addLast("versionHandler", new TcpVersionHandler());
    }

    protected ChannelHandler setupTcpChannel() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                setupTcpSocketChannel(ch);
            }
        };
    }

    protected void setupWsSocketChannel(SocketChannel ch, MetaCache metaCache, TimelyProperties timelyProperties, WebsocketProperties websocketProperties,
                    SslContext sslCtx) {
        ch.pipeline().addLast("ssl", sslCtx.newHandler(ch.alloc()));
        ch.pipeline().addLast("httpServer", new HttpServerCodec());
        ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
        ch.pipeline().addLast("sessionExtractor", new WebSocketFullRequestHandler(authenticationService));
        ch.pipeline().addLast("idle-handler", new IdleStateHandler(websocketProperties.getTimeout(), 0, 0));
        ch.pipeline().addLast("ws-protocol", new WebSocketServerProtocolHandler(WS_PATH, null, true, 65536, false, true));
        ch.pipeline().addLast("wsDecoder", new WebSocketRequestDecoder(authenticationService, securityProperties));
        ch.pipeline().addLast("aggregators", new WSAggregatorsRequestHandler());
        ch.pipeline().addLast("metrics", new WSMetricsRequestHandler(metaCache, timelyProperties));
        ch.pipeline().addLast("query", new WSQueryRequestHandler(dataStore));
        ch.pipeline().addLast("lookup", new WSSearchLookupRequestHandler(dataStore));
        ch.pipeline().addLast("suggest", new WSSuggestRequestHandler(dataStore));
        ch.pipeline().addLast("version", new WSVersionRequestHandler());
        ch.pipeline().addLast("put", new WSMetricPutHandler(dataStore));
        ch.pipeline().addLast("create", new WSCreateSubscriptionRequestHandler(dataStore, dataStoreCache, websocketProperties));
        ch.pipeline().addLast("add", new WSAddSubscriptionRequestHandler());
        ch.pipeline().addLast("remove", new WSRemoveSubscriptionRequestHandler());
        ch.pipeline().addLast("close", new WSCloseSubscriptionRequestHandler());
        ch.pipeline().addLast("error", new WSTimelyExceptionHandler());
    }

    protected ChannelHandler setupWSChannel(MetaCache metaCache, TimelyProperties timelyProperties, WebsocketProperties websocketProperties,
                    SslContext sslCtx) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) {
                setupWsSocketChannel(ch, metaCache, timelyProperties, websocketProperties, sslCtx);
            }
        };
    }

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
}

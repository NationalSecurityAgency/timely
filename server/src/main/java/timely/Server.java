package timely;

import static timely.store.cache.DataStoreCache.NON_CACHED_METRICS;
import static timely.store.cache.DataStoreCache.NON_CACHED_METRICS_LOCK_PATH;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.WriteBufferWaterMark;
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
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSslServerContext;
import io.netty.handler.ssl.OpenSslServerSessionContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.NettyRuntime;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceDiscoveryBuilder;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceInstanceBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import timely.auth.AuthCache;
import timely.auth.VisibilityCache;
import timely.configuration.Configuration;
import timely.configuration.Cors;
import timely.configuration.ServerSsl;
import timely.configuration.SpringBootstrap;
import timely.netty.http.HttpCacheRequestHandler;
import timely.netty.http.HttpMetricPutHandler;
import timely.netty.http.HttpStaticFileServerHandler;
import timely.netty.http.HttpVersionRequestHandler;
import timely.netty.http.NonSslRedirectHandler;
import timely.netty.http.StrictTransportHandler;
import timely.netty.http.TimelyExceptionHandler;
import timely.netty.http.auth.BasicAuthLoginRequestHandler;
import timely.netty.http.auth.X509LoginRequestHandler;
import timely.netty.http.timeseries.HttpAggregatorsRequestHandler;
import timely.netty.http.timeseries.HttpMetricsRequestHandler;
import timely.netty.http.timeseries.HttpQueryRequestHandler;
import timely.netty.http.timeseries.HttpSearchLookupRequestHandler;
import timely.netty.http.timeseries.HttpSuggestRequestHandler;
import timely.netty.tcp.MetricsBufferDecoder;
import timely.netty.tcp.TcpDecoder;
import timely.netty.tcp.TcpPutHandler;
import timely.netty.tcp.TcpVersionHandler;
import timely.netty.udp.UdpDecoder;
import timely.netty.udp.UdpPacketToByteBuf;
import timely.netty.websocket.WSMetricPutHandler;
import timely.netty.websocket.WSVersionRequestHandler;
import timely.netty.websocket.WebSocketFullRequestHandler;
import timely.netty.websocket.WebSocketRequestDecoder;
import timely.netty.websocket.subscription.WSAddSubscriptionRequestHandler;
import timely.netty.websocket.subscription.WSCloseSubscriptionRequestHandler;
import timely.netty.websocket.subscription.WSCreateSubscriptionRequestHandler;
import timely.netty.websocket.subscription.WSRemoveSubscriptionRequestHandler;
import timely.netty.websocket.subscription.WSTimelyExceptionHandler;
import timely.netty.websocket.timeseries.WSAggregatorsRequestHandler;
import timely.netty.websocket.timeseries.WSMetricsRequestHandler;
import timely.netty.websocket.timeseries.WSQueryRequestHandler;
import timely.netty.websocket.timeseries.WSSearchLookupRequestHandler;
import timely.netty.websocket.timeseries.WSSuggestRequestHandler;
import timely.store.DataStore;
import timely.store.DataStoreFactory;
import timely.store.MetaCacheFactory;
import timely.store.cache.DataStoreCache;

public class Server {

    final public static String SERVICE_DISCOVERY_PATH = "/timely/server/instances";
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private static final int EPOLL_MIN_MAJOR_VERSION = 2;
    private static final int EPOLL_MIN_MINOR_VERSION = 6;
    private static final int EPOLL_MIN_PATCH_VERSION = 32;
    private static final String OS_NAME = "os.name";
    private static final String OS_VERSION = "os.version";
    private static final String WS_PATH = "/websocket";

    private CuratorFramework curatorFramework;
    protected static final CountDownLatch LATCH = new CountDownLatch(1);
    static ConfigurableApplicationContext applicationContext;

    private final Configuration config;
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
    protected DataStore dataStore = null;
    protected DataStoreCache dataStoreCache = null;
    protected volatile boolean shutdown = false;
    private final int DEFAULT_EVENT_LOOP_THREADS;

    private String[] zkPaths = new String[] { SERVICE_DISCOVERY_PATH, NON_CACHED_METRICS,
            NON_CACHED_METRICS_LOCK_PATH };

    private static boolean useEpoll() {

        // Should we just return true if this is Linux and if we get an error
        // during Epoll
        // setup handle it there?
        final String os = SystemPropertyUtil.get(OS_NAME).toLowerCase().trim();
        final String[] version = SystemPropertyUtil.get(OS_VERSION).toLowerCase().trim().split("\\.");
        if (os.startsWith("linux") && version.length >= 3) {
            final int major = Integer.parseInt(version[0]);
            if (major > EPOLL_MIN_MAJOR_VERSION) {
                return true;
            } else if (major == EPOLL_MIN_MAJOR_VERSION) {
                final int minor = Integer.parseInt(version[1]);
                if (minor > EPOLL_MIN_MINOR_VERSION) {
                    return true;
                } else if (minor == EPOLL_MIN_MINOR_VERSION) {
                    final int patch = Integer.parseInt(version[2].substring(0, 2));
                    return patch >= EPOLL_MIN_PATCH_VERSION;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    public void registerService(CuratorFramework curatorFramework) {
        try {
            try {
                Stat stat = curatorFramework.checkExists().forPath(SERVICE_DISCOVERY_PATH);
                if (stat == null) {
                    curatorFramework.create().creatingParentContainersIfNeeded().forPath(SERVICE_DISCOVERY_PATH);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage());
            }

            ServerDetails payload = new ServerDetails();
            String host = config.getServer().getIp();
            try {
                InetAddress inetAddr = InetAddress.getByName(host);
                host = inetAddr.getCanonicalHostName();
            } catch (UnknownHostException e) {
                LOG.error(e.getMessage(), e);
            }
            payload.setHost(host);
            payload.setTcpPort(config.getServer().getTcpPort());
            payload.setHttpPort(config.getHttp().getPort());
            payload.setWsPort(config.getWebsocket().getPort());
            payload.setUdpPort(config.getServer().getUdpPort());

            ServiceInstanceBuilder<ServerDetails> builder = ServiceInstance.builder();
            String serviceName = host + ":" + config.getServer().getTcpPort();
            ServiceInstance<ServerDetails> serviceInstance = builder.id(serviceName).name("timely-server")
                    .address(config.getServer().getIp()).port(config.getServer().getTcpPort()).payload(payload).build();

            ServiceDiscovery<ServerDetails> discovery = ServiceDiscoveryBuilder.builder(ServerDetails.class)
                    .client(curatorFramework).basePath(SERVICE_DISCOVERY_PATH).build();
            discovery.start();
            discovery.registerService(serviceInstance);

        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    public static void fatal(String msg, Throwable t) {
        LOG.error(msg, t);
        LATCH.countDown();
    }

    public static void main(String[] args) throws Exception {

        Server.applicationContext = initializeContext(args);
        Configuration conf = Server.applicationContext.getBean(Configuration.class);

        Server server = new Server(conf);
        try {
            server.run();
            LATCH.await();
        } catch (final InterruptedException e) {
            LOG.info("Server shutting down.");
        } catch (Exception e) {
            LOG.error("Error running server.", e);
        } finally {
            try {
                server.shutdown();
            } catch (Exception e) {
                System.exit(1);
            }
        }
    }

    protected static ConfigurableApplicationContext initializeContext(String[] args) {
        SpringApplicationBuilder builder = new SpringApplicationBuilder(SpringBootstrap.class);
        builder.web(WebApplicationType.NONE);
        builder.registerShutdownHook(false);
        return builder.run(args);
    }

    private void shutdownHook() {

        final Runnable shutdownRunner = () -> {
            if (!shutdown) {
                shutdown();
            }
        };
        final Thread hook = new Thread(shutdownRunner, "shutdown-hook-thread");
        Runtime.getRuntime().addShutdownHook(hook);
    }

    public void shutdown() {
        List<ChannelFuture> channelFutures = new ArrayList<>();

        if (tcpChannelHandle != null) {
            LOG.info("Closing tcpChannelHandle");
            channelFutures.add(tcpChannelHandle.close());
        }

        if (httpChannelHandle != null) {
            LOG.info("Closing httpChannelHandle");
            channelFutures.add(httpChannelHandle.close());
        }

        if (wsChannelHandle != null) {
            LOG.info("Closing wsChannelHandle");
            channelFutures.add(wsChannelHandle.close());
        }

        int udpChannel = 1;
        for (Channel c : udpChannelHandleList) {
            LOG.info("Closing udpChannelHandle #" + udpChannel++);
            channelFutures.add(c.close());
        }

        // wait for the channels to shutdown
        channelFutures.forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                LOG.error("Channel:" + f.channel().config() + " -> " + e.getMessage(), e);
            }
        });

        int quietPeriod = config.getServer().getShutdownQuietPeriod();
        List<Future<?>> groupFutures = new ArrayList<>();

        if (tcpBossGroup != null) {
            LOG.info("Shutting down tcpBossGroup");
            groupFutures.add(tcpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (tcpWorkerGroup != null) {
            LOG.info("Shutting down tcpWorkerGroup");
            groupFutures.add(tcpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (httpBossGroup != null) {
            LOG.info("Shutting down httpBossGroup");
            groupFutures.add(httpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (httpWorkerGroup != null) {
            LOG.info("Shutting down httpWorkerGroup");
            groupFutures.add(httpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (wsBossGroup != null) {
            LOG.info("Shutting down wsBossGroup");
            groupFutures.add(wsBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (wsWorkerGroup != null) {
            LOG.info("Shutting down wsWorkerGroup");
            groupFutures.add(wsWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (udpBossGroup != null) {
            LOG.info("Shutting down udpBossGroup");
            groupFutures.add(udpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (udpWorkerGroup != null) {
            LOG.info("Shutting down udpWorkerGroup");
            groupFutures.add(udpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        groupFutures.parallelStream().forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                LOG.error("Group:" + f.toString() + " -> " + e.getMessage(), e);
            }
        });

        try {
            LOG.info("Closing dataStore");
            dataStore.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        try {
            LOG.info("Closing dataStoreCache");
            dataStoreCache.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        try {
            LOG.info("Closing metaCacheFactory");
            MetaCacheFactory.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        try {
            LOG.info("Closing webSocketRequestDecoder subscriptions");
            WebSocketRequestDecoder.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        if (curatorFramework != null) {
            try {
                LOG.info("Closing curatorFramework");
                curatorFramework.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }

        if (applicationContext != null) {
            try {
                LOG.info("Closing applicationContext");
                applicationContext.close();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
        this.shutdown = true;
        LOG.info("Server shut down.");
    }

    public Server(Configuration conf, int eventLoopThreads) throws Exception {

        DEFAULT_EVENT_LOOP_THREADS = eventLoopThreads;
        this.config = conf;
    }

    public Server(Configuration conf) throws Exception {

        DEFAULT_EVENT_LOOP_THREADS = Math.max(1,
                SystemPropertyUtil.getInt("io.netty.eventLoopThreads", NettyRuntime.availableProcessors() * 2));
        this.config = conf;
    }

    private void ensureZkPaths(CuratorFramework curatorFramework, String[] paths) {
        for (String s : paths) {
            try {
                Stat stat = curatorFramework.checkExists().forPath(s);
                if (stat == null) {
                    curatorFramework.create().creatingParentContainersIfNeeded().withMode(CreateMode.PERSISTENT)
                            .forPath(s);
                }
            } catch (Exception e) {
                LOG.info(e.getMessage());
            }
        }
    }

    public void run() throws Exception {

        RetryPolicy retryPolicy = new RetryForever(1000);
        int timeout = Long.valueOf(AccumuloConfiguration.getTimeInMillis(config.getAccumulo().getZookeeperTimeout()))
                .intValue();
        curatorFramework = CuratorFrameworkFactory.newClient(config.getAccumulo().getZookeepers(), timeout, 10000,
                retryPolicy);
        curatorFramework.start();
        ensureZkPaths(curatorFramework, zkPaths);

        int nettyThreads = Math.max(1,
                SystemPropertyUtil.getInt("io.netty.eventLoopThreads", Runtime.getRuntime().availableProcessors() * 2));
        dataStore = DataStoreFactory.create(config, nettyThreads);
        if (config.getCache().isEnabled()) {
            dataStoreCache = new DataStoreCache(curatorFramework, config);
            dataStoreCache.setInternalMetrics(dataStore.getInternalMetrics());
            dataStore.setCache(dataStoreCache);
        }
        // Initialize the MetaCache
        MetaCacheFactory.getCache(config);
        // initialize the auth cache
        AuthCache.configure(config.getSecurity());
        // Initialize the VisibilityCache
        VisibilityCache.init(config);
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
        LOG.info("Using channel class {}", channelClass.getSimpleName());

        final ServerBootstrap tcpServer = new ServerBootstrap();
        tcpServer.group(tcpBossGroup, tcpWorkerGroup);
        tcpServer.channel(channelClass);
        tcpServer.handler(new LoggingHandler());
        tcpServer.childHandler(setupTcpChannel());
        tcpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        tcpServer.option(ChannelOption.SO_BACKLOG, 128);
        tcpServer.option(ChannelOption.SO_KEEPALIVE, true);
        final int tcpPort = config.getServer().getTcpPort();
        final String tcpIp = config.getServer().getIp();
        tcpChannelHandle = tcpServer.bind(tcpIp, tcpPort).sync().channel();
        final String tcpAddress = ((InetSocketAddress) tcpChannelHandle.localAddress()).getAddress().getHostAddress();

        final int httpPort = config.getHttp().getPort();
        final String httpIp = config.getHttp().getIp();
        SslContext sslCtx = createSSLContext(config);
        if (sslCtx instanceof OpenSslServerContext) {
            OpenSslServerContext openssl = (OpenSslServerContext) sslCtx;
            String application = "Timely_" + httpPort;
            OpenSslServerSessionContext opensslCtx = openssl.sessionContext();
            opensslCtx.setSessionCacheEnabled(true);
            opensslCtx.setSessionCacheSize(128);
            opensslCtx.setSessionIdContext(application.getBytes(StandardCharsets.UTF_8));
            opensslCtx.setSessionTimeout(config.getSecurity().getSessionMaxAge());
        }
        final ServerBootstrap httpServer = new ServerBootstrap();
        httpServer.group(httpBossGroup, httpWorkerGroup);
        httpServer.channel(channelClass);
        httpServer.handler(new LoggingHandler());
        httpServer.childHandler(setupHttpChannel(config, sslCtx));
        httpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        httpServer.option(ChannelOption.SO_BACKLOG, 128);
        httpServer.option(ChannelOption.SO_KEEPALIVE, true);
        httpChannelHandle = httpServer.bind(httpIp, httpPort).sync().channel();
        final String httpAddress = ((InetSocketAddress) httpChannelHandle.localAddress()).getAddress().getHostAddress();

        final int wsPort = config.getWebsocket().getPort();
        final String wsIp = config.getWebsocket().getIp();
        final ServerBootstrap wsServer = new ServerBootstrap();
        wsServer.group(wsBossGroup, wsWorkerGroup);
        wsServer.channel(channelClass);
        wsServer.handler(new LoggingHandler());
        wsServer.childHandler(setupWSChannel(sslCtx, config));
        wsServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        wsServer.option(ChannelOption.SO_BACKLOG, 128);
        wsServer.option(ChannelOption.SO_KEEPALIVE, true);
        /* Not sure if next two lines are necessary */
        wsServer.option(ChannelOption.SO_SNDBUF, 1048576);
        wsServer.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(620145, 838860));
        wsChannelHandle = wsServer.bind(wsIp, wsPort).sync().channel();
        final String wsAddress = ((InetSocketAddress) wsChannelHandle.localAddress()).getAddress().getHostAddress();

        final int udpPort = config.getServer().getUdpPort();
        final String udpIp = config.getServer().getIp();

        for (int n = 0; n < DEFAULT_EVENT_LOOP_THREADS; n++) {
            final Bootstrap udpServer = new Bootstrap();
            udpServer.group(udpBossGroup);
            udpServer.channel(datagramChannelClass);
            udpServer.handler(setupUdpChannel());
            udpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
            udpServer.option(EpollChannelOption.SO_REUSEADDR, true);
            udpServer.option(EpollChannelOption.SO_REUSEPORT, true);
            udpChannelHandleList.add(udpServer.bind(udpIp, udpPort).sync().channel());
        }
        registerService(curatorFramework);
        shutdownHook();
        LOG.info(
                "Server started. Listening on {}:{} for TCP traffic, {}:{} for HTTP traffic, {}:{} for WebSocket traffic, and {}:{} for UDP traffic",
                tcpAddress, tcpPort, httpAddress, httpPort, wsAddress, wsPort, wsAddress, udpPort);
    }

    protected SslContext createSSLContext(Configuration config) throws Exception {

        ServerSsl sslCfg = config.getSecurity().getServerSsl();
        Boolean generate = sslCfg.isUseGeneratedKeypair();
        SslContextBuilder ssl;
        if (generate) {
            LOG.warn("Using generated self signed server certificate");
            Date begin = new Date();
            Date end = new Date(begin.getTime() + 86400000);
            SelfSignedCertificate ssc = new SelfSignedCertificate("localhost", begin, end);
            ssl = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey());
        } else {
            String cert = sslCfg.getCertificateFile();
            String key = sslCfg.getKeyFile();
            String keyPass = sslCfg.getKeyPassword();
            if (null == cert || null == key) {
                throw new IllegalArgumentException("Check your SSL properties, something is wrong.");
            }
            ssl = SslContextBuilder.forServer(new File(cert), new File(key), keyPass);
        }

        ssl.ciphers(sslCfg.getUseCiphers());

        // Can't set to REQUIRE because the CORS pre-flight requests will fail.
        ssl.clientAuth(ClientAuth.OPTIONAL);

        Boolean useOpenSSL = sslCfg.isUseOpenssl();
        if (useOpenSSL) {
            ssl.sslProvider(SslProvider.OPENSSL);
        } else {
            ssl.sslProvider(SslProvider.JDK);
        }
        String trustStore = sslCfg.getTrustStoreFile();
        if (null != trustStore) {
            if (!trustStore.isEmpty()) {
                ssl.trustManager(new File(trustStore));
            }
        }
        return ssl.build();
    }

    protected ChannelHandler setupHttpChannel(Configuration config, SslContext sslCtx) {

        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {

                ch.pipeline().addLast("ssl", new NonSslRedirectHandler(config.getHttp(), sslCtx));
                ch.pipeline().addLast("encoder", new HttpResponseEncoder());
                ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                ch.pipeline().addLast("compressor", new HttpContentCompressor());
                ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
                ch.pipeline().addLast("chunker", new ChunkedWriteHandler());
                final Cors corsCfg = config.getHttp().getCors();
                final CorsConfigBuilder ccb;
                if (corsCfg.isAllowAnyOrigin()) {
                    ccb = CorsConfigBuilder.forAnyOrigin();
                } else {
                    ccb = CorsConfigBuilder.forOrigins(corsCfg.getAllowedOrigins().stream().toArray(String[]::new));
                }
                if (corsCfg.isAllowNullOrigin()) {
                    ccb.allowNullOrigin();
                }
                if (corsCfg.isAllowCredentials()) {
                    ccb.allowCredentials();
                }
                corsCfg.getAllowedMethods().stream().map(HttpMethod::valueOf).forEach(ccb::allowedRequestMethods);
                corsCfg.getAllowedHeaders().forEach(ccb::allowedRequestHeaders);
                CorsConfig cors = ccb.build();
                LOG.trace("Cors configuration: {}", cors);
                ch.pipeline().addLast("cors", new CorsHandler(cors));
                ch.pipeline().addLast("queryDecoder",
                        new timely.netty.http.HttpRequestDecoder(config.getSecurity(), config.getHttp()));
                ch.pipeline().addLast("fileServer", new HttpStaticFileServerHandler());
                ch.pipeline().addLast("strict", new StrictTransportHandler(config));
                ch.pipeline().addLast("login", new X509LoginRequestHandler(config.getSecurity(), config.getHttp()));
                ch.pipeline().addLast("doLogin",
                        new BasicAuthLoginRequestHandler(config.getSecurity(), config.getHttp()));
                ch.pipeline().addLast("aggregators", new HttpAggregatorsRequestHandler());
                ch.pipeline().addLast("metrics", new HttpMetricsRequestHandler(config));
                ch.pipeline().addLast("query", new HttpQueryRequestHandler(dataStore));
                ch.pipeline().addLast("search", new HttpSearchLookupRequestHandler(dataStore));
                ch.pipeline().addLast("suggest", new HttpSuggestRequestHandler(dataStore));
                ch.pipeline().addLast("version", new HttpVersionRequestHandler());
                ch.pipeline().addLast("cache", new HttpCacheRequestHandler(dataStoreCache));
                ch.pipeline().addLast("put", new HttpMetricPutHandler(dataStore));
                ch.pipeline().addLast("error", new TimelyExceptionHandler());
            }
        };
    }

    protected ChannelHandler setupUdpChannel() {
        return new ChannelInitializer<DatagramChannel>() {

            @Override
            protected void initChannel(DatagramChannel ch) throws Exception {
                ch.pipeline().addLast("logger", new LoggingHandler());
                ch.pipeline().addLast("packetDecoder", new UdpPacketToByteBuf());
                ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(65536, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new UdpDecoder());
                ch.pipeline().addLast("putHandler", new TcpPutHandler(dataStore));
            }
        };
    }

    protected ChannelHandler setupTcpChannel() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(65536, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new TcpDecoder());
                ch.pipeline().addLast("putHandler", new TcpPutHandler(dataStore));
                ch.pipeline().addLast("versionHandler", new TcpVersionHandler());
            }
        };
    }

    protected ChannelHandler setupWSChannel(SslContext sslCtx, Configuration conf) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("ssl", sslCtx.newHandler(ch.alloc()));
                ch.pipeline().addLast("httpServer", new HttpServerCodec());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
                ch.pipeline().addLast("sessionExtractor", new WebSocketFullRequestHandler());
                ch.pipeline().addLast("idle-handler", new IdleStateHandler(conf.getWebsocket().getTimeout(), 0, 0));
                ch.pipeline().addLast("ws-protocol",
                        new WebSocketServerProtocolHandler(WS_PATH, null, true, 65536, false, true));
                ch.pipeline().addLast("wsDecoder", new WebSocketRequestDecoder(config.getSecurity()));
                ch.pipeline().addLast("aggregators", new WSAggregatorsRequestHandler());
                ch.pipeline().addLast("metrics", new WSMetricsRequestHandler(config));
                ch.pipeline().addLast("query", new WSQueryRequestHandler(dataStore));
                ch.pipeline().addLast("lookup", new WSSearchLookupRequestHandler(dataStore));
                ch.pipeline().addLast("suggest", new WSSuggestRequestHandler(dataStore));
                ch.pipeline().addLast("version", new WSVersionRequestHandler());
                ch.pipeline().addLast("put", new WSMetricPutHandler(dataStore));
                ch.pipeline().addLast("create",
                        new WSCreateSubscriptionRequestHandler(dataStore, dataStoreCache, config));
                ch.pipeline().addLast("add", new WSAddSubscriptionRequestHandler());
                ch.pipeline().addLast("remove", new WSRemoveSubscriptionRequestHandler());
                ch.pipeline().addLast("close", new WSCloseSubscriptionRequestHandler());
                ch.pipeline().addLast("error", new WSTimelyExceptionHandler());
            }
        };

    }

}

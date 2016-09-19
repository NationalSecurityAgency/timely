package timely;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
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
import io.netty.util.internal.SystemPropertyUtil;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import timely.api.response.TimelyException;
import timely.auth.AuthCache;
import timely.auth.VisibilityCache;
import timely.netty.http.HttpMetricPutHandler;
import timely.netty.http.HttpStaticFileServerHandler;
import timely.netty.http.HttpVersionRequestHandler;
import timely.netty.http.NonSecureHttpHandler;
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
import timely.netty.websocket.WebSocketHttpCookieHandler;
import timely.netty.websocket.WebSocketRequestDecoder;
import timely.netty.websocket.subscription.WSAddSubscriptionRequestHandler;
import timely.netty.websocket.subscription.WSCloseSubscriptionRequestHandler;
import timely.netty.websocket.subscription.WSCreateSubscriptionRequestHandler;
import timely.netty.websocket.subscription.WSRemoveSubscriptionRequestHandler;
import timely.netty.websocket.timeseries.WSAggregatorsRequestHandler;
import timely.netty.websocket.timeseries.WSMetricsRequestHandler;
import timely.netty.websocket.timeseries.WSQueryRequestHandler;
import timely.netty.websocket.timeseries.WSSearchLookupRequestHandler;
import timely.netty.websocket.timeseries.WSSuggestRequestHandler;
import timely.store.DataStore;
import timely.store.DataStoreFactory;
import timely.store.MetaCacheFactory;

public class Server {

    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private static final int EPOLL_MIN_MAJOR_VERSION = 2;
    private static final int EPOLL_MIN_MINOR_VERSION = 6;
    private static final int EPOLL_MIN_PATCH_VERSION = 32;
    private static final String OS_NAME = "os.name";
    private static final String OS_VERSION = "os.version";
    private static final String WS_PATH = "/websocket";

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
    protected Channel udpChannelHandle = null;
    protected DataStore dataStore = null;
    protected volatile boolean shutdown = false;

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

    public static void fatal(String msg, Throwable t) {
        LOG.error(msg, t);
        LATCH.countDown();
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = initializeConfiguration(args);

        Server s = new Server(conf);
        s.run();
        try {
            LATCH.await();
        } catch (final InterruptedException e) {
            LOG.info("Server shutting down.");
        } finally {
            s.shutdown();
        }
    }

    protected static Configuration initializeConfiguration(String[] args) {
        applicationContext = new SpringApplicationBuilder(SpringBootstrap.class).web(false).run(args);
        return applicationContext.getBean(Configuration.class);
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

        LOG.info("Closing tcpChannelHandle");
        channelFutures.add(tcpChannelHandle.close());

        LOG.info("Closing httpChannelHandle");
        channelFutures.add(httpChannelHandle.close());

        LOG.info("Closing wsChannelHandle");
        channelFutures.add(wsChannelHandle.close());

        LOG.info("Closing udpChannelHandle");
        channelFutures.add(udpChannelHandle.close());

        // wait for the channels to shutdown
        channelFutures.forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                LOG.error("Error while shutting down channel: {}", f.channel().config());
                LOG.error("{}", e.getMessage());
                e.printStackTrace();
            }
        });

        Integer quietPeriod = config.getServer().getShutdownQuietPeriod();
        List<Future<?>> groupFutures = new ArrayList<>();
        LOG.info("Shutting down tcpBossGroup");
        groupFutures.add(tcpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));

        LOG.info("Shutting down tcpWorkerGroup");
        groupFutures.add(tcpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));

        LOG.info("Shutting down httpBossGroup");
        groupFutures.add(httpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));

        LOG.info("Shutting down httpWorkerGroup");
        groupFutures.add(httpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));

        LOG.info("Shutting down wsBossGroup");
        groupFutures.add(wsBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));

        LOG.info("Shutting down wsWorkerGroup");
        groupFutures.add(wsWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));

        LOG.info("Shutting down udpBossGroup");
        groupFutures.add(udpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));

        LOG.info("Shutting down udpWorkerGroup");
        groupFutures.add(udpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));

        groupFutures.parallelStream().forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                LOG.error("Error while shutting down group: {}", f.toString());
                LOG.error("{}", e.getMessage());
                e.printStackTrace();
            }
        });

        try {
            LOG.info("Flushing datastore.");
            dataStore.flush();
        } catch (TimelyException e) {
            LOG.error("Error flushing to server during shutdown", e);
        }

        LOG.info("Closing MetaCacheFactory");
        MetaCacheFactory.close();

        LOG.info("Closing WebSocketRequestDecoder");
        WebSocketRequestDecoder.close();
        if (applicationContext != null) {
            LOG.info("Closing applicationContext");
            applicationContext.close();
        }
        this.shutdown = true;
        LOG.info("Server shut down.");
    }

    public Server(Configuration conf) throws Exception {

        this.config = conf;
    }

    public void run() throws Exception {

        int nettyThreads = Math.max(1,
                SystemPropertyUtil.getInt("io.netty.eventLoopThreads", Runtime.getRuntime().availableProcessors() * 2));
        dataStore = DataStoreFactory.create(config, nettyThreads);
        // Initialize the MetaCache
        MetaCacheFactory.getCache(config);
        // initialize the auth cache
        AuthCache.setSessionMaxAge(config);
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
        wsChannelHandle = wsServer.bind(wsIp, wsPort).sync().channel();
        final String wsAddress = ((InetSocketAddress) wsChannelHandle.localAddress()).getAddress().getHostAddress();

        final int udpPort = config.getServer().getUdpPort();
        final String udpIp = config.getServer().getIp();
        final Bootstrap udpServer = new Bootstrap();
        udpServer.group(udpBossGroup);
        udpServer.channel(datagramChannelClass);
        udpServer.handler(setupUdpChannel());
        udpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        udpChannelHandle = udpServer.bind(udpIp, udpPort).sync().channel();
        final String udpAddress = ((InetSocketAddress) wsChannelHandle.localAddress()).getAddress().getHostAddress();

        shutdownHook();
        LOG.info(
                "Server started. Listening on {}:{} for TCP traffic, {}:{} for HTTP traffic, {}:{} for WebSocket traffic, and {}:{} for UDP traffic",
                tcpAddress, tcpPort, httpAddress, httpPort, wsAddress, wsPort, udpAddress, udpPort);
    }

    protected SslContext createSSLContext(Configuration config) throws Exception {

        Configuration.Ssl sslCfg = config.getSecurity().getSsl();
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

                ch.pipeline().addLast("ssl", sslCtx.newHandler(ch.alloc()));
                ch.pipeline().addLast("encoder", new HttpResponseEncoder());
                ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                ch.pipeline().addLast("non-secure", new NonSecureHttpHandler(config));
                ch.pipeline().addLast("compressor", new HttpContentCompressor());
                ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(8192));
                ch.pipeline().addLast("chunker", new ChunkedWriteHandler());
                final Configuration.Cors corsCfg = config.getHttp().getCors();
                final CorsConfig.Builder ccb;
                if (corsCfg.isAllowAnyOrigin()) {
                    ccb = new CorsConfig.Builder();
                } else {
                    ccb = new CorsConfig.Builder(corsCfg.getAllowedOrigins().stream().toArray(String[]::new));
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
                ch.pipeline().addLast("queryDecoder", new timely.netty.http.HttpRequestDecoder(config));
                ch.pipeline().addLast("fileServer", new HttpStaticFileServerHandler());
                ch.pipeline().addLast("strict", new StrictTransportHandler(config));
                ch.pipeline().addLast("login", new X509LoginRequestHandler(config));
                ch.pipeline().addLast("doLogin", new BasicAuthLoginRequestHandler(config));
                ch.pipeline().addLast("aggregators", new HttpAggregatorsRequestHandler());
                ch.pipeline().addLast("metrics", new HttpMetricsRequestHandler(config));
                ch.pipeline().addLast("query", new HttpQueryRequestHandler(dataStore));
                ch.pipeline().addLast("search", new HttpSearchLookupRequestHandler(dataStore));
                ch.pipeline().addLast("suggest", new HttpSuggestRequestHandler(dataStore));
                ch.pipeline().addLast("version", new HttpVersionRequestHandler());
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
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(8192, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new UdpDecoder());
                ch.pipeline().addLast(udpWorkerGroup, "putHandler", new TcpPutHandler(dataStore));
            }
        };
    }

    protected ChannelHandler setupTcpChannel() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(8192, true, Delimiters.lineDelimiter()));
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
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(8192));
                ch.pipeline().addLast("sessionExtractor", new WebSocketHttpCookieHandler(config));
                ch.pipeline().addLast("idle-handler", new IdleStateHandler(conf.getWebsocket().getTimeout(), 0, 0));
                ch.pipeline().addLast("ws-protocol", new WebSocketServerProtocolHandler(WS_PATH, null, true));
                ch.pipeline().addLast("wsDecoder", new WebSocketRequestDecoder(config));
                ch.pipeline().addLast("aggregators", new WSAggregatorsRequestHandler());
                ch.pipeline().addLast("metrics", new WSMetricsRequestHandler(config));
                ch.pipeline().addLast("query", new WSQueryRequestHandler(dataStore));
                ch.pipeline().addLast("lookup", new WSSearchLookupRequestHandler(dataStore));
                ch.pipeline().addLast("suggest", new WSSuggestRequestHandler(dataStore));
                ch.pipeline().addLast("version", new WSVersionRequestHandler());
                ch.pipeline().addLast("put", new WSMetricPutHandler(dataStore));
                ch.pipeline().addLast("create", new WSCreateSubscriptionRequestHandler(dataStore, config));
                ch.pipeline().addLast("add", new WSAddSubscriptionRequestHandler());
                ch.pipeline().addLast("remove", new WSRemoveSubscriptionRequestHandler());
                ch.pipeline().addLast("close", new WSCloseSubscriptionRequestHandler());

            }
        };

    }

}

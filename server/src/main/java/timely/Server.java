package timely;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.ServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.SystemPropertyUtil;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.query.response.TimelyException;
import timely.netty.http.HttpAggregatorsRequestHandler;
import timely.netty.http.HttpMetricsRequestHandler;
import timely.netty.http.HttpQueryDecoder;
import timely.netty.http.HttpQueryRequestHandler;
import timely.netty.http.HttpSearchLookupRequestHandler;
import timely.netty.http.HttpSuggestRequestHandler;
import timely.netty.tcp.TcpPutDecoder;
import timely.netty.tcp.TcpPutHandler;
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

    protected static final CountDownLatch LATCH = new CountDownLatch(1);

    private Configuration config = null;
    private EventLoopGroup tcpWorkerGroup = null;
    private EventLoopGroup tcpBossGroup = null;
    private EventLoopGroup httpWorkerGroup = null;
    private EventLoopGroup httpBossGroup = null;
    protected Channel putChannelHandle = null;
    protected Channel queryChannelHandle = null;
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
                    if (patch >= EPOLL_MIN_PATCH_VERSION) {
                        return true;
                    } else {
                        return false;
                    }
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

    private static String usage() {

        return "Server <configFile>";
    }

    public static void fatal(String msg, Throwable t) {
        LOG.error(msg, t);
        LATCH.countDown();
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.err.println(usage());
        }
        final File conf = new File(args[0]);
        if (!conf.canRead()) {
            throw new RuntimeException("Configuration file does not exist or cannot be read");
        }
        Server s = new Server(conf);
        try {
            LATCH.await();
        } catch (final InterruptedException e) {
            LOG.info("Server shutting down.");
        } finally {
            s.shutdown();
        }
    }

    private void shutdownHook() {

        final Runnable shutdownRunner = new Runnable() {

            @Override
            public void run() {
                if (!shutdown) {
                    shutdown();
                }
            }
        };
        final Thread hook = new Thread(shutdownRunner, "shutdown-hook-thread");
        Runtime.getRuntime().addShutdownHook(hook);
    }

    public void shutdown() {
        try {
            putChannelHandle.close().sync();
        } catch (final Exception e) {
            LOG.error("Error shutting down tcp channel", e);
        }
        try {
            queryChannelHandle.close().sync();
        } catch (final Exception e) {
            LOG.error("Error shutting down http channel", e);
        }
        try {
            tcpBossGroup.shutdownGracefully().sync();
        } catch (final Exception e) {
            LOG.error("Error closing Netty TCP boss thread group", e);
        }
        try {
            tcpWorkerGroup.shutdownGracefully().sync();
        } catch (final Exception e) {
            LOG.error("Error closing Netty TCP worker thread group", e);
        }
        try {
            httpBossGroup.shutdownGracefully().sync();
        } catch (final Exception e) {
            LOG.error("Error closing Netty HTTP boss thread group", e);
        }
        try {
            httpWorkerGroup.shutdownGracefully().sync();
        } catch (final Exception e) {
            LOG.error("Error closing Netty HTTP worker thread group", e);
        }
        try {
            dataStore.flush();
        } catch (TimelyException e) {
            LOG.error("Error flushing to server during shutdown", e);
        }
        MetaCacheFactory.close();
        this.shutdown = true;
        LOG.info("Server shut down.");
    }

    public Server(File conf) throws Exception {

        config = new Configuration(conf);
        int nettyThreads = Math.max(1,
                SystemPropertyUtil.getInt("io.netty.eventLoopThreads", Runtime.getRuntime().availableProcessors() * 2));
        dataStore = DataStoreFactory.create(config, nettyThreads);
        // Initialize the MetaCache
        MetaCacheFactory.getCache(config);
        final boolean useEpoll = useEpoll();
        Class<? extends ServerSocketChannel> channelClass = null;
        if (useEpoll) {
            tcpWorkerGroup = new EpollEventLoopGroup();
            tcpBossGroup = new EpollEventLoopGroup();
            httpWorkerGroup = new EpollEventLoopGroup();
            httpBossGroup = new EpollEventLoopGroup();
            channelClass = EpollServerSocketChannel.class;
        } else {
            tcpWorkerGroup = new NioEventLoopGroup();
            tcpBossGroup = new NioEventLoopGroup();
            httpWorkerGroup = new NioEventLoopGroup();
            httpBossGroup = new NioEventLoopGroup();
            channelClass = NioServerSocketChannel.class;
        }
        LOG.info("Using channel class {}", channelClass.getSimpleName());

        final ServerBootstrap putServer = new ServerBootstrap();
        putServer.group(tcpBossGroup, tcpWorkerGroup);
        putServer.channel(channelClass);
        putServer.handler(new LoggingHandler());
        putServer.childHandler(setupTcpChannel());
        putServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        putServer.option(ChannelOption.SO_BACKLOG, 128);
        putServer.option(ChannelOption.SO_KEEPALIVE, true);
        final int putPort = Integer.parseInt(config.get(Configuration.PUT_PORT));
        putChannelHandle = putServer.bind(putPort).sync().channel();
        final String putAddress = ((InetSocketAddress) putChannelHandle.localAddress()).getAddress().getHostAddress();

        final ServerBootstrap queryServer = new ServerBootstrap();
        queryServer.group(httpBossGroup, httpWorkerGroup);
        queryServer.channel(channelClass);
        queryServer.handler(new LoggingHandler());
        queryServer.childHandler(setupHttpChannel(config));
        queryServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        queryServer.option(ChannelOption.SO_BACKLOG, 128);
        queryServer.option(ChannelOption.SO_KEEPALIVE, true);
        final int queryPort = Integer.parseInt(config.get(Configuration.QUERY_PORT));
        queryChannelHandle = queryServer.bind(queryPort).sync().channel();
        final String queryAddress = ((InetSocketAddress) queryChannelHandle.localAddress()).getAddress()
                .getHostAddress();

        shutdownHook();
        LOG.info("Server started. Listening on {}:{} for TCP traffic and {}:{} for HTTP traffic", putAddress, putPort,
                queryAddress, queryPort);
    }

    protected ChannelHandler setupHttpChannel(Configuration config) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                ch.pipeline().addLast("encoder", new HttpResponseEncoder());
                ch.pipeline().addLast("deflater", new HttpContentCompressor());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(8192));
                final CorsConfig.Builder ccb;
                if (Boolean.parseBoolean(config.get(Configuration.CORS_ALLOW_ANY_ORIGIN))) {
                    ccb = new CorsConfig.Builder();
                } else {
                    String origins = config.get(Configuration.CORS_ALLOWED_ORIGINS);
                    ccb = new CorsConfig.Builder(origins.split(","));
                }
                if (Boolean.parseBoolean(config.get(Configuration.CORS_ALLOW_NULL_ORIGIN))) {
                    ccb.allowNullOrigin();
                }
                if (Boolean.parseBoolean(config.get(Configuration.CORS_ALLOW_CREDENTIALS))) {
                    ccb.allowCredentials();
                }
                if (null != config.get(Configuration.CORS_ALLOWED_METHODS)) {
                    String[] methods = config.get(Configuration.CORS_ALLOWED_METHODS).split(",");
                    HttpMethod[] m = new HttpMethod[methods.length];
                    for (int i = 0; i < methods.length; i++) {
                        m[i] = HttpMethod.valueOf(methods[i]);
                    }
                    ccb.allowedRequestMethods(m);
                }
                if (null != config.get(Configuration.CORS_ALLOWED_HEADERS)) {
                    ccb.allowedRequestHeaders(config.get(Configuration.CORS_ALLOWED_HEADERS).split(","));
                }
                CorsConfig cors = ccb.build();
                LOG.trace("Cors configuration: {}", cors);
                ch.pipeline().addLast("cors", new CorsHandler(cors));
                ch.pipeline().addLast("queryDecoder", new HttpQueryDecoder());
                ch.pipeline().addLast("aggregators", new HttpAggregatorsRequestHandler());
                ch.pipeline().addLast("metrics", new HttpMetricsRequestHandler(config));
                ch.pipeline().addLast("query", new HttpQueryRequestHandler(dataStore));
                ch.pipeline().addLast("search", new HttpSearchLookupRequestHandler(dataStore));
                ch.pipeline().addLast("suggest", new HttpSuggestRequestHandler(dataStore));
            }
        };
    }

    protected ChannelHandler setupTcpChannel() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(8192, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new TcpPutDecoder());
                ch.pipeline().addLast("putHandler", new TcpPutHandler(dataStore));
            }
        };
    }

}

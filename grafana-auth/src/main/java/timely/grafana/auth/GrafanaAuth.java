package timely.grafana.auth;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
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
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSslServerContext;
import io.netty.handler.ssl.OpenSslServerSessionContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.util.internal.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import timely.auth.AuthCache;
import timely.balancer.configuration.ClientSsl;
import timely.balancer.connection.http.HttpClientPool;
import timely.client.http.HttpClient;
import timely.configuration.ServerSsl;
import timely.grafana.auth.configuration.GrafanaAuthConfiguration;
import timely.grafana.auth.configuration.SpringBootstrap;
import timely.grafana.auth.netty.http.GrafanaRelayHandler;
import timely.grafana.auth.netty.http.GrafanaRequestDecoder;
import timely.netty.http.HttpStaticFileServerHandler;
import timely.netty.http.NonSslRedirectHandler;
import timely.netty.http.TimelyExceptionHandler;
import timely.netty.http.auth.X509LoginRequestHandler;

public class GrafanaAuth {

    private static final Logger LOG = LoggerFactory.getLogger(GrafanaAuth.class);
    private static final int EPOLL_MIN_MAJOR_VERSION = 2;
    private static final int EPOLL_MIN_MINOR_VERSION = 6;
    private static final int EPOLL_MIN_PATCH_VERSION = 32;
    private static final String OS_NAME = "os.name";
    private static final String OS_VERSION = "os.version";
    protected static final CountDownLatch LATCH = new CountDownLatch(1);
    static ConfigurableApplicationContext applicationContext;

    private final GrafanaAuthConfiguration grafanaAuthConfig;
    private EventLoopGroup httpWorkerGroup = null;
    private EventLoopGroup httpBossGroup = null;
    protected Channel httpChannelHandle = null;
    protected volatile boolean shutdown = false;

    public GrafanaAuth(GrafanaAuthConfiguration grafanaAuthConfig) throws Exception {
        this.grafanaAuthConfig = grafanaAuthConfig;
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

        if (httpChannelHandle != null) {
            LOG.info("Closing httpChannelHandle");
            channelFutures.add(httpChannelHandle.close());
        }

        // wait for the channels to shutdown
        channelFutures.forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                LOG.error("Channel:" + f.channel().config() + " -> " + e.getMessage(), e);
            }
        });

        int quietPeriod = grafanaAuthConfig.getShutdownQuietPeriod();
        List<Future<?>> groupFutures = new ArrayList<>();

        if (httpBossGroup != null) {
            LOG.info("Shutting down httpBossGroup");
            groupFutures.add(httpBossGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        if (httpWorkerGroup != null) {
            LOG.info("Shutting down httpWorkerGroup");
            groupFutures.add(httpWorkerGroup.shutdownGracefully(quietPeriod, 10, TimeUnit.SECONDS));
        }

        groupFutures.parallelStream().forEach(f -> {
            try {
                f.get();
            } catch (final Exception e) {
                LOG.error("Group:" + f.toString() + " -> " + e.getMessage(), e);
            }
        });

        if (applicationContext != null) {
            LOG.info("Closing applicationContext");
            applicationContext.close();
        }

        this.shutdown = true;
        LOG.info("Server shut down.");
    }

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

    protected SslContext createSSLContext(GrafanaAuthConfiguration config) throws Exception {

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

    protected SSLContext createSSLClientContext(GrafanaAuthConfiguration config) throws Exception {

        ClientSsl ssl = config.getSecurity().getClientSsl();
        return HttpClient.getSSLContext(ssl.getTrustStoreFile(), ssl.getTrustStoreType(), ssl.getTrustStorePassword(),
                ssl.getKeyFile(), ssl.getKeyType(), ssl.getKeyPassword());
    }

    public void run() throws Exception {

        AuthCache.configure(grafanaAuthConfig.getSecurity());
        final boolean useEpoll = useEpoll();
        Class<? extends ServerSocketChannel> channelClass;
        if (useEpoll) {
            httpWorkerGroup = new EpollEventLoopGroup();
            httpBossGroup = new EpollEventLoopGroup();
            channelClass = EpollServerSocketChannel.class;
        } else {
            httpWorkerGroup = new NioEventLoopGroup();
            httpBossGroup = new NioEventLoopGroup();
            channelClass = NioServerSocketChannel.class;
        }
        LOG.info("Using channel class {}", channelClass.getSimpleName());

        final int httpPort = grafanaAuthConfig.getHttp().getPort();
        final String httpIp = grafanaAuthConfig.getHttp().getIp();
        SslContext sslCtx = createSSLContext(grafanaAuthConfig);
        if (sslCtx instanceof OpenSslServerContext) {
            OpenSslServerContext openssl = (OpenSslServerContext) sslCtx;
            String application = "GrafanaAuth_" + httpPort;
            OpenSslServerSessionContext opensslCtx = openssl.sessionContext();
            opensslCtx.setSessionCacheEnabled(true);
            opensslCtx.setSessionCacheSize(128);
            opensslCtx.setSessionIdContext(application.getBytes(StandardCharsets.UTF_8));
            opensslCtx.setSessionTimeout(grafanaAuthConfig.getSecurity().getSessionMaxAge());
        }
        SSLContext clientSSLContext = createSSLClientContext(grafanaAuthConfig);
        final ServerBootstrap httpServer = new ServerBootstrap();
        httpServer.group(httpBossGroup, httpWorkerGroup);
        httpServer.channel(channelClass);
        httpServer.handler(new LoggingHandler());
        HttpClientPool httpClientPool = new HttpClientPool(grafanaAuthConfig.getSecurity(), grafanaAuthConfig.getHttp(),
                clientSSLContext);
        httpServer.childHandler(setupHttpChannel(grafanaAuthConfig, sslCtx, httpClientPool));
        httpServer.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        httpServer.option(ChannelOption.SO_BACKLOG, 128);
        httpServer.option(ChannelOption.SO_KEEPALIVE, true);
        httpChannelHandle = httpServer.bind(httpIp, httpPort).sync().channel();
        final String httpAddress = ((InetSocketAddress) httpChannelHandle.localAddress()).getAddress().getHostAddress();

        shutdownHook();
        LOG.info("Server started. Listening on {}:{} for HTTP traffic", httpAddress, httpPort);
    }

    public static void main(String[] args) throws Exception {

        GrafanaAuth.applicationContext = GrafanaAuth.initializeContext(args);
        GrafanaAuthConfiguration grafanaAuthConf = GrafanaAuth.applicationContext
                .getBean(GrafanaAuthConfiguration.class);

        GrafanaAuth grafanaAuth = new GrafanaAuth(grafanaAuthConf);
        try {
            grafanaAuth.run();
            LATCH.await();
        } catch (final InterruptedException e) {
            LOG.info("Server shutting down.");
        } catch (Exception e) {
            LOG.error("Error running server.", e);
        } finally {
            try {
                grafanaAuth.shutdown();
            } catch (Exception e) {
                System.exit(1);
            }
        }
    }

    protected ChannelHandler setupHttpChannel(GrafanaAuthConfiguration config, SslContext sslCtx,
            HttpClientPool httpClientPool) {

        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {

                ch.pipeline().addLast("ssl", new NonSslRedirectHandler(config.getHttp(), sslCtx));
                ch.pipeline().addLast("encoder", new HttpResponseEncoder());
                ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                ch.pipeline().addLast("compressor", new HttpContentCompressor());
                ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                // high maximum contentLength so that grafana snapshots can be delivered
                // might not be necessary if inbound chunking (while proxying) is handled
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(2097152));
                ch.pipeline().addLast("chunker", new ChunkedWriteHandler());
                ch.pipeline().addLast("grafanaDecoder",
                        new GrafanaRequestDecoder(config.getSecurity(), config.getHttp()));
                ch.pipeline().addLast("fileServer", new HttpStaticFileServerHandler());
                ch.pipeline().addLast("login", new X509LoginRequestHandler(config.getSecurity(), config.getHttp()));
                ch.pipeline().addLast("httpRelay", new GrafanaRelayHandler(config, httpClientPool));
                ch.pipeline().addLast("error", new TimelyExceptionHandler());
            }
        };
    }
}

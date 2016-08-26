package timely;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.ssl.SslContext;

import timely.netty.tcp.MetricsBufferDecoder;
import timely.netty.tcp.TcpDecoder;
import timely.test.TestCaptureRequestHandler;

public class TestServer extends Server {

    private TestCaptureRequestHandler tcpRequests = new TestCaptureRequestHandler();
    private TestCaptureRequestHandler httpRequests = new TestCaptureRequestHandler();

    public TestServer(Configuration conf) throws Exception {
        super(conf);
    }

    @Override
    protected ChannelHandler setupHttpChannel(Configuration config, SslContext sslCtx) {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("ssl", sslCtx.newHandler(ch.alloc()));
                ch.pipeline().addLast("decompressor", new HttpContentDecompressor());
                ch.pipeline().addLast("decoder", new HttpRequestDecoder());
                ch.pipeline().addLast("aggregator", new HttpObjectAggregator(8192));
                ch.pipeline().addLast("queryDecoder", new timely.netty.http.HttpRequestDecoder(config));
                ch.pipeline().addLast("capture", httpRequests);
            }
        };
    }

    @Override
    protected ChannelHandler setupTcpChannel() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("buffer", new MetricsBufferDecoder());
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(8192, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new TcpDecoder());
                ch.pipeline().addLast("capture", tcpRequests);
            }
        };
    }

    public TestCaptureRequestHandler getPutRequests() {
        return tcpRequests;
    }

    public TestCaptureRequestHandler getHttpRequests() {
        return httpRequests;
    }

}

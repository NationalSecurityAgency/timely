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

import java.io.File;

import timely.netty.http.HttpQueryDecoder;
import timely.netty.tcp.TcpPutDecoder;
import timely.test.TestCaptureRequestHandler;

public class TestServer extends Server {

    private TestCaptureRequestHandler putRequests = new TestCaptureRequestHandler();
    private TestCaptureRequestHandler httpRequests = new TestCaptureRequestHandler();

    public TestServer(File conf) throws Exception {
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
                ch.pipeline().addLast("queryDecoder", new HttpQueryDecoder(config));
                ch.pipeline().addLast("capture", httpRequests);
            }
        };
    }

    @Override
    protected ChannelHandler setupTcpChannel() {
        return new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast("frame", new DelimiterBasedFrameDecoder(8192, true, Delimiters.lineDelimiter()));
                ch.pipeline().addLast("putDecoder", new TcpPutDecoder());
                ch.pipeline().addLast("capture", putRequests);
            }
        };
    }

    public TestCaptureRequestHandler getPutRequests() {
        return putRequests;
    }

    public TestCaptureRequestHandler getHttpRequests() {
        return httpRequests;
    }

}

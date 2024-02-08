package timely.netty.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.OptionalSslHandler;
import io.netty.handler.ssl.SslContext;
import timely.configuration.Http;
import timely.netty.Constants;

public class NonSslRedirectHandler extends OptionalSslHandler implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NonSslRedirectHandler.class);
    private final String redirectAddress;

    public NonSslRedirectHandler(Http http, SslContext sslContext) {
        super(sslContext);
        String timelyHost = http.getHost();
        int timelyPort = http.getPort();
        String path = http.getRedirectPath();
        redirectAddress = "https://" + timelyHost + ":" + timelyPort + path;
    }

    @Override
    protected String newSslHandlerName() {
        return "ssl";
    }

    @Override
    protected String newNonSslHandlerName() {
        return "ssl";
    }

    @Override
    protected ChannelHandler newNonSslHandler(ChannelHandlerContext context) {
        return new ChannelInboundHandlerAdapter() {

            private HttpResponseEncoder encoder = new HttpResponseEncoder();

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                LOG.trace("Received non-SSL request, returning redirect");
                FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.MOVED_PERMANENTLY, Unpooled.EMPTY_BUFFER);
                response.headers().set(HttpHeaderNames.LOCATION, redirectAddress);
                LOG.trace(Constants.LOG_RETURNING_RESPONSE, response);
                encoder.write(ctx, response, ctx.voidPromise());
                ctx.flush();
            }
        };
    }

}

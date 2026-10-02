package timely.netty.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.OptionalSslHandler;
import io.netty.handler.ssl.SslContext;
import timely.common.configuration.HttpProperties;
import timely.netty.Constants;

public class NonSslForbiddenHandler extends OptionalSslHandler implements TimelyHttpHandler {

    private static final Logger log = LoggerFactory.getLogger(NonSslForbiddenHandler.class);

    public NonSslForbiddenHandler(HttpProperties httpProperties, SslContext sslContext) {
        super(sslContext);
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
                log.trace("Received non-SSL request, returning redirect");
                FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.FORBIDDEN, Unpooled.EMPTY_BUFFER);
                log.trace(Constants.LOG_RETURNING_RESPONSE, response);
                encoder.write(ctx, response, ctx.voidPromise());
                ctx.flush();
            }
        };
    }

}

package timely.netty.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.OptionalSslHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import timely.netty.Constants;

public class NonSslForbiddenHandler extends OptionalSslHandler {

    private static final Logger log = LoggerFactory.getLogger(NonSslForbiddenHandler.class);

    public NonSslForbiddenHandler(SslContext sslContext) {
        super(sslContext);
    }

    @Override
    protected String newSslHandlerName() {
        return "ssl";
    }

    @Override
    protected String newNonSslHandlerName() {
        return "nonSslForbidden";
    }

    @Override
    protected ChannelHandler newNonSslHandler(ChannelHandlerContext context) {
        return new ChannelInboundHandlerAdapter() {

            @Override
            public void channelRead(ChannelHandlerContext ctx, Object msg) {
                try {
                    log.trace("Received non-SSL request, returning forbidden");
                    String response = "HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n";
                    log.trace(Constants.LOG_RETURNING_RESPONSE, response);
                    ctx.writeAndFlush(Unpooled.copiedBuffer(response, CharsetUtil.UTF_8)).addListener(ChannelFutureListener.CLOSE);
                } finally {
                    ReferenceCountUtil.release(msg);
                }
            }
        };
    }
}

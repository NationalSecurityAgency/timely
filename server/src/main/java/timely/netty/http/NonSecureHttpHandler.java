package timely.netty.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.ssl.NotSslRecordException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.netty.Constants;

public class NonSecureHttpHandler extends ChannelInboundHandlerAdapter implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(NonSecureHttpHandler.class);
    private final String redirectAddress;

    public NonSecureHttpHandler(Configuration conf) {
        String timelyHost = conf.getHttp().getHost();
        int timelyPort = conf.getHttp().getPort();
        String path = conf.getHttp().getRedirectPath();
        redirectAddress = "https://" + timelyHost + ":" + timelyPort + path;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        ctx.pipeline().remove("ssl");
        if (cause instanceof NotSslRecordException) {
            LOG.trace("Received non-SSL request, returning redirect");
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1,
                    HttpResponseStatus.MOVED_PERMANENTLY, Unpooled.EMPTY_BUFFER);
            response.headers().set(Names.LOCATION, redirectAddress);
            LOG.trace(Constants.LOG_RETURNING_RESPONSE, response);
            ctx.writeAndFlush(response);
        }
    }

}

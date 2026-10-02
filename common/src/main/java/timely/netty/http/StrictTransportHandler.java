package timely.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpResponse;
import timely.common.configuration.HttpProperties;

public class StrictTransportHandler extends ChannelOutboundHandlerAdapter {

    public static final String HSTS_HEADER_NAME = "Strict-Transport-Security";
    private final String HSTS_HEADER_VALUE;

    public StrictTransportHandler(HttpProperties httpProperties) {
        this.HSTS_HEADER_VALUE = String.format("max-age=%d; includeSubDomains; preload", httpProperties.getStrictTransportMaxAge());
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Check if the connection is SSL/TLS encrypted
        boolean isHttps = ctx.channel().pipeline().get(io.netty.handler.ssl.SslHandler.class) != null;
        if (isHttps && msg instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) msg;
            // Add the HSTS header only if it isn't already present
            if (!response.headers().contains(HSTS_HEADER_NAME)) {
                response.headers().set(HSTS_HEADER_NAME, HSTS_HEADER_VALUE);
            }
        }
        // Forward the message to the next handler in the pipeline
        super.write(ctx, msg, promise);
    }
}

package timely.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import timely.Configuration;
import timely.api.response.StrictTransportResponse;
import timely.api.response.TimelyException;

public class StrictTransportHandler extends SimpleChannelInboundHandler<StrictTransportResponse> {

    public static final String HSTS_HEADER_NAME = "Strict-Transport-Security";
    private String hstsMaxAge = "max-age=";

    public StrictTransportHandler(Configuration conf) {
        long maxAge = conf.getHttp().getStrictTransportMaxAge();
        hstsMaxAge = "max-age=" + maxAge;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, StrictTransportResponse msg) throws Exception {
        TimelyException e = new TimelyException(HttpResponseStatus.NOT_FOUND.code(),
                "Returning HTTP Strict Transport Security response", null, null);
        e.addResponseHeader(HSTS_HEADER_NAME, hstsMaxAge);
        // Don't call sendHttpError from here, throw an error instead and let
        // the exception handler catch it.
        throw e;
    }

}

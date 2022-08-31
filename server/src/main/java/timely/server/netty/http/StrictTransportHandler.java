package timely.server.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import timely.api.response.StrictTransportResponse;
import timely.api.response.TimelyException;
import timely.common.configuration.HttpProperties;

public class StrictTransportHandler extends SimpleChannelInboundHandler<StrictTransportResponse> {

    public static final String HSTS_HEADER_NAME = "Strict-Transport-Security";
    private HttpProperties httpProperties;

    public StrictTransportHandler(HttpProperties httpProperties) {
        this.httpProperties = httpProperties;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, StrictTransportResponse msg) throws Exception {
        TimelyException e = new TimelyException(HttpResponseStatus.NOT_FOUND.code(), "Returning HTTP Strict Transport Security response", null, null);
        String headerValue = "max-age=" + httpProperties.getStrictTransportMaxAge();
        e.addResponseHeader(HSTS_HEADER_NAME, headerValue);
        // Don't call sendHttpError from here, throw an error instead and let
        // the exception handler catch it.
        throw e;
    }

}

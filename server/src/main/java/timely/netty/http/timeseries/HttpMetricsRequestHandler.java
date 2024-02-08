package timely.netty.http.timeseries;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.timeseries.MetricsResponse;
import timely.configuration.Configuration;
import timely.netty.http.TimelyHttpHandler;

public class HttpMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> implements TimelyHttpHandler {

    private Configuration conf = null;

    public HttpMetricsRequestHandler(Configuration conf) {
        this.conf = conf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest msg) throws Exception {
        MetricsResponse r = new MetricsResponse(conf);
        String acceptHeader = msg.getRequestHeader(HttpHeaderNames.ACCEPT.toString());
        sendResponse(ctx, r.toHttpResponse(acceptHeader));
    }

}

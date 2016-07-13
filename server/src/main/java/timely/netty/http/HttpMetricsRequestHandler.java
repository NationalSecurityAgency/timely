package timely.netty.http;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaders.Names;
import timely.Configuration;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.timeseries.MetricsResponse;

public class HttpMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> implements TimelyHttpHandler {

    private Configuration conf = null;

    public HttpMetricsRequestHandler(Configuration conf) {
        this.conf = conf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest msg) throws Exception {
        MetricsResponse r = new MetricsResponse(conf);
        String acceptHeader = msg.getRequestHeaders().get(Names.ACCEPT);
        sendResponse(ctx, r.toHttpResponse(acceptHeader));
    }

}

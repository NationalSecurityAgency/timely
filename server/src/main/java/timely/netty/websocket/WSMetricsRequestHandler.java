package timely.netty.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.Configuration;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.timeseries.MetricsResponse;
import timely.netty.http.TimelyHttpHandler;

public class WSMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> implements TimelyHttpHandler {

    private Configuration conf = null;

    public WSMetricsRequestHandler(Configuration conf) {
        this.conf = conf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest m) throws Exception {
        MetricsResponse r = new MetricsResponse(conf);
        sendResponse(ctx, r.toWebSocketResponse("application/json"));
    }

}

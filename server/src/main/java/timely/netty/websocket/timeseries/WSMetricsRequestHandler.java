package timely.netty.websocket.timeseries;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.timeseries.MetricsResponse;
import timely.configuration.Configuration;

public class WSMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> {

    private Configuration conf = null;

    public WSMetricsRequestHandler(Configuration conf) {
        this.conf = conf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest m) throws Exception {
        MetricsResponse r = new MetricsResponse(conf);
        ctx.writeAndFlush(r.toWebSocketResponse("application/json"));
    }

}

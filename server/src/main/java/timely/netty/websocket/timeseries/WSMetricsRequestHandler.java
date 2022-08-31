package timely.netty.websocket.timeseries;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.timeseries.MetricsResponse;
import timely.common.configuration.TimelyProperties;
import timely.store.MetaCache;

public class WSMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> {

    private MetaCache metaCache;
    private TimelyProperties timelyProperties;

    public WSMetricsRequestHandler(MetaCache metaCache, TimelyProperties timelyProperties) {
        this.metaCache = metaCache;
        this.timelyProperties = timelyProperties;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest m) throws Exception {
        MetricsResponse r = new MetricsResponse(metaCache, timelyProperties);
        ctx.writeAndFlush(r.toWebSocketResponse("application/json"));
    }

}

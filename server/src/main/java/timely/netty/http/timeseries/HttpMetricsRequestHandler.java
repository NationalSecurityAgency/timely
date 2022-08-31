package timely.netty.http.timeseries;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpHeaderNames;
import timely.api.request.timeseries.MetricsRequest;
import timely.api.response.timeseries.MetricsResponse;
import timely.common.configuration.TimelyProperties;
import timely.netty.http.TimelyHttpHandler;
import timely.store.MetaCache;

public class HttpMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> implements TimelyHttpHandler {

    private TimelyProperties timelyProperties;
    private MetaCache metaCache;

    public HttpMetricsRequestHandler(MetaCache metaCache, TimelyProperties conf) {
        this.metaCache = metaCache;
        this.timelyProperties = conf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest msg) throws Exception {
        MetricsResponse r = new MetricsResponse(metaCache, timelyProperties);
        String acceptHeader = msg.getRequestHeader(HttpHeaderNames.ACCEPT.toString());
        sendResponse(ctx, r.toHttpResponse(acceptHeader));
    }

}

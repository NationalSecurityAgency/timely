package timely.netty.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.MetricRequest;
import timely.store.DataStore;

public class WSMetricPutHandler extends SimpleChannelInboundHandler<MetricRequest> {

    private final DataStore dataStore;

    public WSMetricPutHandler(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricRequest m) throws Exception {
        this.dataStore.store(m.getMetric());
    }

}

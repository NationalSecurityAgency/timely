package timely.netty.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.model.Metric;
import timely.store.DataStore;

public class WSMetricPutHandler extends SimpleChannelInboundHandler<Metric> {

    private final DataStore dataStore;

    public WSMetricPutHandler(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Metric m) throws Exception {
        this.dataStore.store(m);
    }

}

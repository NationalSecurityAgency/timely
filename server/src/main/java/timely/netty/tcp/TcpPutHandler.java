package timely.netty.tcp;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.request.MetricRequest;
import timely.model.Metric;
import timely.netty.Constants;
import timely.store.DataStore;

public class TcpPutHandler extends SimpleChannelInboundHandler<MetricRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(TcpPutHandler.class);
    private static final String LOG_ERR_MSG = "Error storing put metric: {}";
    private static final String ERR_MSG = "Error storing put metric: ";
    private DataStore store;

    public TcpPutHandler(DataStore store) {
        this.store = store;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricRequest msg) throws Exception {
        LOG.trace("Received {}", msg);
        try {
            store.store(msg.getMetric());
        } catch (Exception e) {
            LOG.error(LOG_ERR_MSG, msg, e);
            ChannelFuture cf = ctx.writeAndFlush(Unpooled.copiedBuffer((ERR_MSG + e.getMessage() + "\n")
                    .getBytes(StandardCharsets.UTF_8)));
            if (!cf.isSuccess()) {
                LOG.error(Constants.ERR_WRITING_RESPONSE, cf.cause());
            }
        }
    }

}

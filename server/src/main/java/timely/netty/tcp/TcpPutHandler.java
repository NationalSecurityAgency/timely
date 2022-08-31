package timely.netty.tcp;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.MetricRequest;
import timely.netty.Constants;
import timely.server.component.DataStore;

public class TcpPutHandler extends SimpleChannelInboundHandler<MetricRequest> {

    private static final Logger log = LoggerFactory.getLogger(TcpPutHandler.class);
    private static final String LOG_ERR_MSG = "Error storing put metric: {}";
    private static final String ERR_MSG = "Error storing put metric: ";
    private DataStore store;

    public TcpPutHandler(DataStore store) {
        this.store = store;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricRequest msg) throws Exception {
        log.trace("Received {}", msg);
        try {
            store.store(msg.getMetric());
        } catch (Exception e) {
            log.error(LOG_ERR_MSG, msg, e);
            ChannelFuture cf = ctx.writeAndFlush(Unpooled.copiedBuffer((ERR_MSG + e.getMessage() + "\n").getBytes(StandardCharsets.UTF_8)));
            if (!cf.isSuccess()) {
                log.error(Constants.ERR_WRITING_RESPONSE, cf.cause());
            }
        }
    }

}

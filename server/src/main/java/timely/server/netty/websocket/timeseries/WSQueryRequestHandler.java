package timely.server.netty.websocket.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.server.netty.http.timeseries.HttpQueryRequestHandler;
import timely.server.store.DataStore;
import timely.util.JsonUtil;

public class WSQueryRequestHandler extends SimpleChannelInboundHandler<QueryRequest> {

    private static final Logger log = LoggerFactory.getLogger(HttpQueryRequestHandler.class);
    private final DataStore dataStore;

    public WSQueryRequestHandler(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, QueryRequest msg) throws Exception {
        try {
            String response = JsonUtil.getObjectMapper().writeValueAsString(dataStore.query(msg));
            ctx.writeAndFlush(new TextWebSocketFrame(response));
        } catch (TimelyException e) {
            if (e.getMessage().contains("No matching tags")) {
                log.trace(e.getMessage());
            } else {
                log.error(e.getMessage(), e);
            }
            ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
        }
    }

}

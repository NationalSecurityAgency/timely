package timely.netty.websocket.timeseries;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.response.TimelyException;
import timely.server.component.DataStore;
import timely.util.JsonUtil;

public class WSSearchLookupRequestHandler extends SimpleChannelInboundHandler<SearchLookupRequest> {

    private static final Logger log = LoggerFactory.getLogger(WSSearchLookupRequestHandler.class);
    private DataStore dataStore;

    public WSSearchLookupRequestHandler(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SearchLookupRequest msg) throws Exception {
        try {
            String response = JsonUtil.getObjectMapper().writeValueAsString(dataStore.lookup(msg));
            ctx.writeAndFlush(new TextWebSocketFrame(response));
        } catch (TimelyException e) {
            log.error(e.getMessage(), e);
            ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
        }
    }

}

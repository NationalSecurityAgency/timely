package timely.netty.websocket.timeseries;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.timeseries.SuggestRequest;
import timely.api.response.TimelyException;
import timely.netty.http.timeseries.HttpSuggestRequestHandler;
import timely.store.DataStore;
import timely.util.JsonUtil;

public class WSSuggestRequestHandler extends SimpleChannelInboundHandler<SuggestRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSuggestRequestHandler.class);
    private final DataStore dataStore;

    public WSSuggestRequestHandler(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SuggestRequest msg) throws Exception {
        try {
            String response = JsonUtil.getObjectMapper().writeValueAsString(dataStore.suggest(msg));
            ctx.writeAndFlush(new TextWebSocketFrame(response));
        } catch (TimelyException e) {
            LOG.error(e.getMessage(), e);
            ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
        }
    }

}

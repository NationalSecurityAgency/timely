package timely.netty.http.timeseries;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.timeseries.SuggestRequest;
import timely.api.response.TimelyException;
import timely.netty.Constants;
import timely.netty.http.TimelyHttpHandler;
import timely.store.DataStore;
import timely.util.JsonUtil;

public class HttpSuggestRequestHandler extends SimpleChannelInboundHandler<SuggestRequest>
        implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpSuggestRequestHandler.class);
    private final DataStore dataStore;

    public HttpSuggestRequestHandler(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SuggestRequest msg) throws Exception {
        byte[] buf = null;
        try {
            buf = JsonUtil.getObjectMapper().writeValueAsBytes(dataStore.suggest(msg));
        } catch (TimelyException e) {
            LOG.error(e.getMessage(), e);
            this.sendHttpError(ctx, e);
            return;
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(buf));
        response.headers().set(Names.CONTENT_TYPE, Constants.JSON_TYPE);
        response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
        sendResponse(ctx, response);
    }

}

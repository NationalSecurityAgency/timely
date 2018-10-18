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
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.TimelyException;
import timely.netty.Constants;
import timely.netty.http.TimelyHttpHandler;
import timely.store.DataStore;
import timely.util.JsonUtil;

public class HttpQueryRequestHandler extends SimpleChannelInboundHandler<QueryRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpQueryRequestHandler.class);
    private final DataStore dataStore;

    public HttpQueryRequestHandler(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, QueryRequest msg) throws Exception {
        byte[] buf;
        try {
            buf = JsonUtil.getObjectMapper().writeValueAsBytes(dataStore.query(msg));
        } catch (TimelyException e) {
            if (e.getMessage().contains("No matching tags")) {
                LOG.trace(e.getMessage());
            } else {
                LOG.error(e.getMessage(), e);
            }
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

package timely.server.netty.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import timely.api.request.MetricRequest;
import timely.api.response.TimelyException;
import timely.netty.Constants;
import timely.netty.http.TimelyHttpHandler;
import timely.server.store.DataStore;

public class HttpMetricPutHandler extends SimpleChannelInboundHandler<MetricRequest> implements TimelyHttpHandler {

    private static final Logger log = LoggerFactory.getLogger(HttpMetricPutHandler.class);
    private final DataStore dataStore;

    public HttpMetricPutHandler(DataStore dataStore) {
        this.dataStore = dataStore;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricRequest m) throws Exception {
        try {
            this.dataStore.store(m.getMetric());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), e.getMessage(), "", e));
            return;
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.EMPTY_BUFFER);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, Constants.JSON_TYPE);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        sendResponse(ctx, response);
    }

}

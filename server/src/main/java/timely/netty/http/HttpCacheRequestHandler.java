package timely.netty.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import timely.api.request.CacheRequest;
import timely.api.response.TimelyException;
import timely.netty.Constants;
import timely.store.cache.DataStoreCache;
import timely.util.JsonUtil;

public class HttpCacheRequestHandler extends SimpleChannelInboundHandler<CacheRequest> implements TimelyHttpHandler {

    private DataStoreCache cache = null;

    public HttpCacheRequestHandler(DataStoreCache cache) {
        this.cache = cache;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, CacheRequest v) throws Exception {
        byte[] buf;
        try {
            buf = JsonUtil.getObjectMapper().writeValueAsBytes(cache.getCacheStatus());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            this.sendHttpError(ctx, new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "Error getting cache status", "Error getting cache status", e));
            return;
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(buf));
        response.headers().set(Names.CONTENT_TYPE, Constants.JSON_TYPE);
        response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
        sendResponse(ctx, response);
    }

}

package timely.netty.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.response.TimelyException;
import timely.netty.Constants;
import timely.util.JsonUtil;

import com.fasterxml.jackson.core.JsonProcessingException;

public interface TimelyHttpHandler {

    Logger LOG = LoggerFactory.getLogger(TimelyHttpHandler.class);

    default void sendHttpError(ChannelHandlerContext ctx, TimelyException e) throws JsonProcessingException {
        LOG.error("Error in pipeline, response code: {}, message: {}", e.getCode(), e.getMessage());
        byte[] buf = JsonUtil.getObjectMapper().writeValueAsBytes(
                "ResponseCode: " + e.getCode() + " Message: " + e.getMessage());
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(e
                .getCode()), Unpooled.copiedBuffer(buf));
        response.headers().set(Names.CONTENT_TYPE, Constants.JSON_TYPE);
        response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
        e.getResponseHeaders().entrySet().forEach(entry -> response.headers().set(entry.getKey(), entry.getValue()));
        // Send the error response
        sendResponse(ctx, response);
    }

    default void sendResponse(ChannelHandlerContext ctx, Object msg) {
        ChannelFuture f = ctx.writeAndFlush(msg);
        LOG.trace(Constants.LOG_RETURNING_RESPONSE, msg);
        if (!f.isSuccess()) {
            LOG.error(Constants.ERR_WRITING_RESPONSE, f.cause());
        }
    }

}

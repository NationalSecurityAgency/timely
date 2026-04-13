package timely.netty.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import timely.api.response.TimelyException;
import timely.api.response.TimelyExceptionResponse;
import timely.netty.Constants;
import timely.util.JsonUtil;

public interface TimelyHttpHandler {

    Logger log = LoggerFactory.getLogger(TimelyHttpHandler.class);

    default void sendHttpError(ChannelHandlerContext ctx, TimelyException e) throws JsonProcessingException {
        try {
            log.error("Error in pipeline, response code: {}, message: {}", e.getCode(), e.getMessage());
            byte[] buf = JsonUtil.getObjectMapper().writeValueAsBytes(new TimelyExceptionResponse(e));
            FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.valueOf(e.getCode()), Unpooled.copiedBuffer(buf));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, Constants.JSON_TYPE);
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            e.getResponseHeaders().entrySet().forEach(entry -> response.headers().set(entry.getKey(), entry.getValue()));
            // Send the error response
            sendResponse(ctx, response);
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    default void sendResponse(ChannelHandlerContext ctx, Object msg) {
        ChannelFuture cf = ctx.writeAndFlush(msg);
        log.trace(Constants.LOG_RETURNING_RESPONSE, msg);
        if (!cf.isSuccess()) {
            Throwable t = cf.cause();
            String message = (t == null) ? "" : t.getMessage();
            log.error(String.format(Constants.ERR_WRITING_RESPONSE, message), cf.cause());
        }
    }

}

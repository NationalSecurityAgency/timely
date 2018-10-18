package timely.netty.http;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import timely.api.request.VersionRequest;
import timely.netty.Constants;

public class HttpVersionRequestHandler extends SimpleChannelInboundHandler<VersionRequest>
        implements TimelyHttpHandler {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, VersionRequest v) throws Exception {
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(VersionRequest.VERSION.getBytes(StandardCharsets.UTF_8)));
        response.headers().set(Names.CONTENT_TYPE, Constants.TEXT_TYPE);
        response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
        sendResponse(ctx, response);
    }

}

package timely.server.netty.http.timeseries;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import timely.api.request.timeseries.AggregatorsRequest;
import timely.api.response.timeseries.AggregatorsResponse;
import timely.netty.Constants;
import timely.netty.http.TimelyHttpHandler;
import timely.util.JsonUtil;

public class HttpAggregatorsRequestHandler extends SimpleChannelInboundHandler<AggregatorsRequest> implements TimelyHttpHandler {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AggregatorsRequest msg) throws Exception {
        byte[] buf = JsonUtil.getObjectMapper().writeValueAsBytes(AggregatorsResponse.RESPONSE);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(buf));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, Constants.JSON_TYPE);
        response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
        sendResponse(ctx, response);
    }

}

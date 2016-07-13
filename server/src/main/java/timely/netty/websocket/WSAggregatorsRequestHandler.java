package timely.netty.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import timely.api.request.timeseries.AggregatorsRequest;
import timely.api.response.timeseries.AggregatorsResponse;
import timely.netty.http.TimelyHttpHandler;
import timely.util.JsonUtil;

public class WSAggregatorsRequestHandler extends SimpleChannelInboundHandler<AggregatorsRequest> implements
        TimelyHttpHandler {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AggregatorsRequest agg) throws Exception {
        String json = JsonUtil.getObjectMapper().writeValueAsString(AggregatorsResponse.RESPONSE);
        ctx.writeAndFlush(new TextWebSocketFrame(json));
    }

}

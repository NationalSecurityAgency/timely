package timely.netty.websocket.timeseries;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import timely.api.request.timeseries.AggregatorsRequest;
import timely.api.response.timeseries.AggregatorsResponse;
import timely.util.JsonUtil;

public class WSAggregatorsRequestHandler extends SimpleChannelInboundHandler<AggregatorsRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AggregatorsRequest agg) throws Exception {
        String json = JsonUtil.getObjectMapper().writeValueAsString(AggregatorsResponse.RESPONSE);
        ctx.writeAndFlush(new TextWebSocketFrame(json));
    }

}

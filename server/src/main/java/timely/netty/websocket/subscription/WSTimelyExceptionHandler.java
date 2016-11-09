package timely.netty.websocket.subscription;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import timely.api.response.TimelyException;

public class WSTimelyExceptionHandler extends SimpleChannelInboundHandler<TimelyException> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TimelyException e) throws Exception {
        ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) throws Exception {
        ctx.writeAndFlush(new CloseWebSocketFrame(1008, e.getMessage()));
    }

}

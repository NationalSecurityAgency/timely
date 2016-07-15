package timely.netty.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import timely.api.request.VersionRequest;

public class WSVersionRequestHandler extends SimpleChannelInboundHandler<VersionRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, VersionRequest msg) throws Exception {
        ctx.writeAndFlush(new TextWebSocketFrame(VersionRequest.VERSION));
    }

}

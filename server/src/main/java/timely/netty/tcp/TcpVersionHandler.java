package timely.netty.tcp;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.nio.charset.StandardCharsets;

import timely.api.request.Version;

public class TcpVersionHandler extends SimpleChannelInboundHandler<Version> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Version v) throws Exception {
        final ByteBuf response = ctx.alloc().buffer();
        response.writeBytes(v.getVersion().getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(response);
    }

}

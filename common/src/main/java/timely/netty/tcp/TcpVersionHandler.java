package timely.netty.tcp;

import java.nio.charset.StandardCharsets;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.VersionRequest;

public class TcpVersionHandler extends SimpleChannelInboundHandler<VersionRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, VersionRequest v) throws Exception {
        final ByteBuf response = ctx.alloc().buffer();
        response.writeBytes(v.getVersion().getBytes(StandardCharsets.UTF_8));
        ctx.writeAndFlush(response);
    }

}

package timely.netty.http;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import timely.api.query.response.TimelyException;

@Sharable
public class TimelyExceptionHandler extends SimpleChannelInboundHandler<TimelyException> implements TimelyHttpHandler {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TimelyException msg) throws Exception {
        this.sendHttpError(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        if (cause instanceof TimelyException) {
            this.sendHttpError(ctx, (TimelyException) cause);
        } else if (null != cause.getCause() && cause.getCause() instanceof TimelyException) {
            this.sendHttpError(ctx, (TimelyException) cause.getCause());
        } else {
            TimelyException e = new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    cause.getMessage(), "");
            this.sendHttpError(ctx, e);
        }
    }

}

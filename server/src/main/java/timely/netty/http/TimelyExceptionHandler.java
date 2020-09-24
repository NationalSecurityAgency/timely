package timely.netty.http;

import javax.net.ssl.SSLHandshakeException;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.response.TimelyException;

@Sharable
public class TimelyExceptionHandler extends SimpleChannelInboundHandler<TimelyException> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TimelyExceptionHandler.class);
    private boolean ignoreSslHandshakeErrors = false;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TimelyException msg) throws Exception {
        this.sendHttpError(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        // ignore SSLHandshakeException when using a self-signed server certificate
        if (ignoreSslHandshakeErrors && cause.getCause() instanceof SSLHandshakeException) {
            return;
        }
        LOG.error("Exception in pipeline", cause);
        if (cause instanceof TimelyException) {
            this.sendHttpError(ctx, (TimelyException) cause);
        } else if (null != cause.getCause() && cause.getCause() instanceof TimelyException) {
            this.sendHttpError(ctx, (TimelyException) cause.getCause());
        } else {
            TimelyException e = new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), cause.getMessage(),
                    "");
            this.sendHttpError(ctx, e);
        }
    }

    public TimelyExceptionHandler setIgnoreSslHandshakeErrors(boolean ignoreSslHandshakeErrors) {
        this.ignoreSslHandshakeErrors = ignoreSslHandshakeErrors;
        return this;
    }
}

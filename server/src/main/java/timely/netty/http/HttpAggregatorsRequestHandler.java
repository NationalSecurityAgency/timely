package timely.netty.http;

import java.nio.charset.StandardCharsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import timely.api.query.request.AggregatorsRequest;
import timely.api.query.response.AggregatorsResponse;
import timely.netty.Constants;
import timely.sample.aggregators.Avg;
import timely.sample.aggregators.Count;
import timely.sample.aggregators.Dev;
import timely.sample.aggregators.Max;
import timely.sample.aggregators.Min;
import timely.sample.aggregators.Sum;
import timely.util.JsonUtil;

public class HttpAggregatorsRequestHandler extends SimpleChannelInboundHandler<AggregatorsRequest> implements
        TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpAggregatorsRequestHandler.class);
    private static final AggregatorsResponse RESPONSE = new AggregatorsResponse();

    static {
        RESPONSE.addAggregator(Avg.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Dev.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Max.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Min.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Sum.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Count.class.getSimpleName().toLowerCase());
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AggregatorsRequest msg) throws Exception {
        byte[] buf = JsonUtil.getObjectMapper().writeValueAsBytes(RESPONSE);
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(buf));
        response.headers().set(Names.CONTENT_TYPE, Constants.JSON_TYPE);
        response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
        sendResponse(ctx, response);
        LOG.trace(Constants.LOG_RETURNING_RESPONSE, new String(buf, StandardCharsets.UTF_8));
    }

}

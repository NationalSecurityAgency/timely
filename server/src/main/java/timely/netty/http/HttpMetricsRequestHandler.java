package timely.netty.http;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;

import timely.Configuration;
import timely.api.query.request.MetricsRequest;
import timely.api.query.response.MetricsResponse;
import timely.netty.Constants;
import timely.util.JsonUtil;

public class HttpMetricsRequestHandler extends SimpleChannelInboundHandler<MetricsRequest> implements TimelyHttpHandler {

    private static final Logger LOG = LoggerFactory.getLogger(HttpMetricsRequestHandler.class);
    private Configuration conf = null;

    public HttpMetricsRequestHandler(Configuration conf) {
        this.conf = conf;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MetricsRequest msg) throws Exception {
        MetricsResponse r = new MetricsResponse(conf);
        String acceptValue = msg.getRequestHeaders().get(Names.ACCEPT);
        MediaType negotiatedType = MediaType.TEXT_HTML;
        if (null != acceptValue) {
            List<MediaType> requestedTypes = MediaType.parseMediaTypes(acceptValue);
            MediaType.sortBySpecificityAndQuality(requestedTypes);
            LOG.trace("Acceptable response types: {}", MediaType.toString(requestedTypes));
            for (MediaType t : requestedTypes) {
                if (t.includes(MediaType.TEXT_HTML)) {
                    negotiatedType = MediaType.TEXT_HTML;
                    LOG.trace("{} allows HTML", t.toString());
                    break;
                }
                if (t.includes(MediaType.APPLICATION_JSON)) {
                    negotiatedType = MediaType.APPLICATION_JSON;
                    LOG.trace("{} allows JSON", t.toString());
                    break;
                }
            }
        }
        byte[] buf = null;
        Object responseType = Constants.HTML_TYPE;
        if (negotiatedType.equals(MediaType.APPLICATION_JSON)) {
            buf = r.generateJson(JsonUtil.getObjectMapper()).getBytes(StandardCharsets.UTF_8);
            responseType = Constants.JSON_TYPE;
        } else {
            buf = r.generateHtml().toString().getBytes(StandardCharsets.UTF_8);
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(buf));
        response.headers().set(Names.CONTENT_TYPE, responseType);
        response.headers().set(Names.CONTENT_LENGTH, response.content().readableBytes());
        sendResponse(ctx, response);
    }

}

package timely.netty.tcp;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import timely.api.model.Metric;
import timely.api.model.Tag;
import timely.auth.VisibilityCache;

public class TcpPutDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(TcpPutDecoder.class);

    private static final String VISIBILITY_TAG = "viz=";
    private static final int VISIBILITY_TAG_LENGTH = VISIBILITY_TAG.length();

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        // put specification
        //
        // put <metricName> <timestamp> <value> <tagK=tagV> <tagK=tagV> ...
        ByteBuf buf = in.readBytes(in.readableBytes());
        if (buf == Unpooled.EMPTY_BUFFER) {
            return;
        }
        String input = new String(buf.array(), UTF_8);
        LOG.trace("Received input {}", input);
        try {
            String[] parts = input.split(" ");
            if (parts.length == 1 && parts[0].toLowerCase().startsWith("version")) {
                final ByteBuf response = ctx.alloc().buffer();
                response.writeBytes("0.0.1\n".getBytes(UTF_8));
                ctx.writeAndFlush(response);
                return;
            }
            Metric put = new Metric();
            put.setMetric(parts[1]);
            long ts = Long.parseLong(parts[2]);
            if (ts < 9999999999L) {
                ts *= 1000;
            }
            put.setTimestamp(ts);
            put.setValue(Double.valueOf(parts[3]));
            String part;
            for (int i = 4; i < parts.length; i++) {
                part = parts[i];
                if (part.startsWith(VISIBILITY_TAG) && part.length() > VISIBILITY_TAG_LENGTH) {
                    put.setVisibility(VisibilityCache.getColumnVisibility(part.substring(VISIBILITY_TAG_LENGTH)));
                } else if (!part.isEmpty()) {
                    put.addTag(new Tag(parts[i]));
                }
            }
            out.add(put);
            LOG.trace("Converted {} to {}", input, put);
        } catch (NumberFormatException e) {
            LOG.error("Error converting input '{}' to put metric", input, e);
        }
    }

}

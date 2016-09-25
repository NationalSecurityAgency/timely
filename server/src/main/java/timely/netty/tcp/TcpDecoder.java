package timely.netty.tcp;

import static java.nio.charset.StandardCharsets.UTF_8;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.annotation.AnnotationResolver;
import timely.api.request.TcpRequest;

public class TcpDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(TcpDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        ByteBuf buf = in.readBytes(in.readableBytes());
        try {
            if (buf == Unpooled.EMPTY_BUFFER) {
                return;
            }
            final String input;
            if (buf.hasArray()) {
                input = new String(buf.array(), UTF_8);
            } else {
                input = buf.toString(UTF_8);
            }
            if (StringUtils.isEmpty(input)) {
                LOG.warn("Received no input");
                return;
            }
            LOG.trace("Received input: {}", input);

            String[] parts = input.split(" ");
            String operation;
            if (parts.length == 0 && !StringUtils.isEmpty(input)) {
                operation = input;
            } else {
                operation = parts[0];
            }
            TcpRequest tcp = null;
            try {
                tcp = AnnotationResolver.getClassForTcpOperation(operation);
            } catch (Exception e) {
                LOG.error("Error getting class for operation: " + operation, e);
            }
            if (null == tcp) {
                LOG.error("Unknown tcp operation: " + parts[0]);
                return;
            }
            tcp.parse(input);
            out.add(tcp);
            LOG.trace("Converted {} to {}", input, tcp);
        } finally {
            buf.release();
        }
    }

}

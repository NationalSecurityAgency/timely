package timely.netty.tcp;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import timely.api.annotation.AnnotationResolver;
import timely.api.request.TcpRequest;

public class TcpDecoder extends ByteToMessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(TcpDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {

        ByteBuf buf = in.readBytes(in.readableBytes());
        String input = null;
        try {
            if (buf == Unpooled.EMPTY_BUFFER) {
                return;
            }
            if (buf.hasArray()) {
                input = new String(buf.array(), UTF_8);
            } else {
                input = buf.toString(UTF_8);
            }
            if (StringUtils.isEmpty(input)) {
                log.warn("Received no input");
                return;
            }
            log.trace("Received input: {}", input);

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
                log.error("Error getting class for operation: " + operation, e);
            }
            if (null == tcp) {
                log.error("Unknown tcp operation:[{}] in input:[{}]", parts[0], input);
                return;
            }
            tcp.parse(input);
            out.add(tcp);
            log.trace("Converted {} to {}", input, tcp);
        } catch (Exception e) {
            log.error("{} parsing line:[{}]", e.getMessage(), input);
        } finally {
            buf.release();
        }
    }

}

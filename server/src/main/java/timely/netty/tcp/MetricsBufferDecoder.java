package timely.netty.tcp;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.flatbuffer.Metrics;
import timely.api.model.Metric;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class MetricsBufferDecoder extends ByteToMessageDecoder {

    private static final Logger LOG = LoggerFactory.getLogger(MetricsBufferDecoder.class);

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        if (in.readableBytes() < 8) {
            return;
        }
        LOG.debug("Received {} bytes", in.readableBytes());
        boolean isFlatBuffer = false;
        try {
            isFlatBuffer = Metrics.MetricsBufferHasIdentifier(in.slice().nioBuffer());
        } catch (Exception e) {
            LOG.warn("Error checking for identifier: " + e.getClass().getSimpleName());
            isFlatBuffer = false;
        }
        if (!isFlatBuffer) {
            LOG.debug("Input is not a Flatbuffer");
            out.add(in.readBytes(in.readableBytes()));
        } else {
            LOG.debug("Input is a Flatbuffer");
            ByteBuf copy = in.readBytes(in.readableBytes());
            try {
                Metrics metrics = Metrics.getRootAsMetrics(copy.nioBuffer());
                int length = metrics.metricsLength();
                for (int i = 0; i < length; i++) {
                    Metric m = new Metric();
                    m.parseMetric(metrics.metrics(i));
                    LOG.debug("Returning {}", m);
                    out.add(m);
                }
            } catch (Exception e) {
                LOG.warn("Error decoding byte[] with Google Flatbuffers: " + e.getMessage());
                return;
            } finally {
                copy.release();
            }
        }
    }

}

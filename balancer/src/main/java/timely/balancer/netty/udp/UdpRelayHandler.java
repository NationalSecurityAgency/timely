package timely.balancer.netty.udp;

import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.MetricRequest;
import timely.api.request.UdpRequest;
import timely.balancer.component.MetricResolver;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.udp.UdpClientPool;
import timely.client.udp.UdpClient;
import timely.netty.Constants;

public class UdpRelayHandler extends SimpleChannelInboundHandler<UdpRequest> {

    private static final Logger log = LoggerFactory.getLogger(UdpRelayHandler.class);
    private static final String LOG_ERR_MSG = "Error storing put metric: {}";
    private static final String ERR_MSG = "Error storing put metric: ";

    private MetricResolver metricResolver;
    private UdpClientPool udpClientPool;

    public UdpRelayHandler(MetricResolver metricResolver, UdpClientPool udpClientPool) {
        this.metricResolver = metricResolver;
        this.udpClientPool = udpClientPool;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, UdpRequest msg) throws Exception {
        log.trace("Received {}", msg);
        try {
            MetricRequest metricRequest = (MetricRequest) msg;
            String metricName = metricRequest.getMetric().getName();
            Pair<TimelyBalancedHost,UdpClient> keyClientPair = null;
            try {
                keyClientPair = getClient(metricName, true);
                keyClientPair.getRight().write(metricRequest.getLine() + "\n");
                keyClientPair.getRight().flush();
            } finally {
                if (keyClientPair != null && keyClientPair.getLeft() != null && keyClientPair.getRight() != null) {
                    udpClientPool.returnObject(keyClientPair.getLeft(), keyClientPair.getRight());
                }
            }

        } catch (Exception e) {
            log.error(LOG_ERR_MSG, msg, e);
            ChannelFuture cf = ctx.writeAndFlush(Unpooled.copiedBuffer((ERR_MSG + e.getMessage() + "\n").getBytes(StandardCharsets.UTF_8)));
            if (!cf.isSuccess()) {
                log.error(Constants.ERR_WRITING_RESPONSE, cf.cause());
            }
        }
    }

    private Pair<TimelyBalancedHost,UdpClient> getClient(String metric, boolean metricRequest) {
        UdpClient client = null;
        TimelyBalancedHost k = null;
        int failures = 0;
        while (client == null) {
            try {
                k = (metricRequest == true) ? metricResolver.getHostPortKeyIngest(metric) : metricResolver.getHostPortKey(metric);
                client = udpClientPool.borrowObject(k);
            } catch (Exception e1) {
                failures++;
                client = null;
                log.error(e1.getMessage(), e1);
                try {
                    Thread.sleep(failures < 10 ? 500 : 60000);
                } catch (InterruptedException e2) {
                    // nothing
                }
            }
        }
        return Pair.of(k, client);
    }
}

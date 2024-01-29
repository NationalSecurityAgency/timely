package timely.balancer.netty.tcp;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import timely.api.request.MetricRequest;
import timely.api.request.TcpRequest;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.connection.tcp.TcpClientPool;
import timely.balancer.resolver.BalancedMetricResolver;
import timely.balancer.resolver.MetricResolver;
import timely.client.tcp.TcpClient;
import timely.netty.Constants;

public class TcpRelayHandler extends SimpleChannelInboundHandler<TcpRequest> {

    private static final Logger LOG = LoggerFactory.getLogger(TcpRelayHandler.class);
    private static final String LOG_ERR_MSG = "Error storing put metric: {}";
    private static final String ERR_MSG = "Error storing put metric: ";

    private BalancedMetricResolver metricResolver;
    private List<TcpClientPool> tcpClientPools;

    public TcpRelayHandler(MetricResolver metricResolver, List<TcpClientPool> tcpClientPools) {
        this.metricResolver = (BalancedMetricResolver) metricResolver;
        this.tcpClientPools = tcpClientPools;
    }

    private TcpClientPool tcpClientPool() {
        int x = Math.toIntExact(Thread.currentThread().getId() % tcpClientPools.size());
        return tcpClientPools.get(x);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TcpRequest msg) throws Exception {
        LOG.trace("Received {}", msg);
        try {
            String line;
            Pair<TimelyBalancedHost,TcpClient> keyClientPair = null;
            try {
                if (msg instanceof MetricRequest) {
                    String metricName = ((MetricRequest) msg).getMetric().getName();
                    line = ((MetricRequest) msg).getLine();
                    keyClientPair = getClient(metricName, true);
                } else {
                    // Version request
                    line = "version";
                    keyClientPair = getClient(null, false);
                }
                keyClientPair.getRight().write(line + "\n");
            } finally {
                if (keyClientPair != null && keyClientPair.getLeft() != null && keyClientPair.getRight() != null) {
                    tcpClientPool().returnObject(keyClientPair.getLeft(), keyClientPair.getRight());
                }
            }

        } catch (IOException e) {
            LOG.error(LOG_ERR_MSG, msg, e);
            ChannelFuture cf = ctx.writeAndFlush(Unpooled.copiedBuffer((ERR_MSG + e.getMessage() + "\n").getBytes(StandardCharsets.UTF_8)));
            if (!cf.isSuccess()) {
                LOG.error(Constants.ERR_WRITING_RESPONSE, cf.cause());
            }
        }
    }

    private Pair<TimelyBalancedHost,TcpClient> getClient(String metric, boolean metricRequest) {
        TcpClient client = null;
        TimelyBalancedHost k = null;
        int failures = 0;
        while (client == null) {
            try {
                k = (metricRequest == true) ? metricResolver.getHostPortKeyIngest(metric) : metricResolver.getHostPortKey(metric);
                client = tcpClientPool().borrowObject(k);
            } catch (Exception e1) {
                failures++;
                client = null;
                if (failures % 10 == 0) {
                    LOG.error(e1.getMessage(), e1);
                }
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

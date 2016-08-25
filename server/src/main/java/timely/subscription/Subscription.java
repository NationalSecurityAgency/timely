package timely.subscription;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.util.concurrent.ScheduledFuture;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.Configuration;
import timely.api.response.TimelyException;
import timely.store.DataStore;

public class Subscription {

    private static final Logger LOG = LoggerFactory.getLogger(Subscription.class);
    private static final Map<String, MetricScanner> METRICS = new ConcurrentHashMap<>();

    private final String sessionId;
    private final DataStore store;
    private final ChannelHandlerContext ctx;
    private final ScheduledFuture<?> ping;
    private final Integer lag;
    private final String subscriptionId;

    public Subscription(String subscriptionId, String sessionId, DataStore store, ChannelHandlerContext ctx,
            Configuration conf) {
        this.subscriptionId = subscriptionId;
        this.sessionId = sessionId;
        this.store = store;
        this.ctx = ctx;
        this.lag = conf.getWebsocket().getSubscriptionLag();
        // send a websocket ping at half the timeout interval.
        int rate = conf.getWebsocket().getTimeout() / 2;
        this.ping = this.ctx.executor().scheduleAtFixedRate(() -> {
            LOG.trace("Sending ping on channel {}", ctx.channel());
            ctx.writeAndFlush(new PingWebSocketFrame());
        }, rate, rate, TimeUnit.SECONDS);
    }

    public void addMetric(String metric, Map<String, String> tags, long startTime, long delay) throws TimelyException {
        LOG.debug("Adding metric scanner for subscription {}", this.subscriptionId);
        MetricScanner m = new MetricScanner(this.subscriptionId, this.sessionId, store, metric, tags, startTime, delay,
                lag, ctx);
        METRICS.put(metric, m);
        m.start();
    }

    public void removeMetric(String metric) {
        MetricScanner m = METRICS.remove(metric);
        if (null != m) {
            m.close();
            try {
                m.join();
            } catch (InterruptedException e) {
                LOG.error("Error closing MetricScanner", e);
            }
        }
    }

    public void close() {
        LOG.info("Closing subscriptions for {}", this.subscriptionId);
        this.ping.cancel(false);
        METRICS.forEach((k, v) -> {
            v.close();
            try {
                v.join();
            } catch (InterruptedException e) {
                LOG.error("Error closing MetricScanner", e);
            }
        });
        METRICS.clear();
    }

}

package timely.subscription;

import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.PingWebSocketFrame;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.request.AuthenticatedRequest;
import timely.api.response.TimelyException;
import timely.configuration.Configuration;
import timely.store.DataStore;
import timely.store.cache.DataStoreCache;

public class Subscription {

    private static final Logger LOG = LoggerFactory.getLogger(Subscription.class);
    private static final Map<String, MetricScanner> METRICS = new ConcurrentHashMap<>();

    private final String sessionId;
    private final DataStore store;
    private final DataStoreCache cache;
    private final ChannelHandlerContext ctx;
    private final ScheduledFuture<?> ping;
    private final int lag;
    private final int scannerBatchSize;
    private final int flushIntervalSeconds;
    private final int scannerReadAhead;
    private final int subscriptionBatchSize;
    private final String subscriptionId;
    private LinkedList<String> deadMetricScannerQueue = new LinkedList<>();

    public Subscription(String subscriptionId, String sessionId, DataStore store, DataStoreCache cache,
            ChannelHandlerContext ctx, Configuration conf) {
        this.subscriptionId = subscriptionId;
        this.sessionId = sessionId;
        this.store = store;
        this.cache = cache;
        this.ctx = ctx;
        this.lag = conf.getWebsocket().getSubscriptionLag();
        this.scannerBatchSize = conf.getWebsocket().getScannerBatchSize();
        this.flushIntervalSeconds = conf.getWebsocket().getFlushIntervalSeconds();
        this.scannerReadAhead = conf.getWebsocket().getScannerReadAhead();
        this.subscriptionBatchSize = conf.getWebsocket().getSubscriptionBatchSize();
        // send a websocket ping at half the timeout interval.
        int rate = conf.getWebsocket().getTimeout() / 2;
        this.ping = this.ctx.executor().scheduleAtFixedRate(() -> {
            LOG.trace("[{}] Sending ping on channel {}", subscriptionId, ctx.channel());
            ctx.writeAndFlush(new PingWebSocketFrame());
            cleanupCompletedMetrics();
        }, rate, rate, TimeUnit.SECONDS);
    }

    public void addMetric(AuthenticatedRequest request, String metric, Map<String, String> tags, long startTime,
            long endTime, long delay) throws TimelyException {
        LOG.debug("[{}] Adding metric scanner", this.subscriptionId);
        MetricScanner m = new MetricScanner(this, this.subscriptionId, this.sessionId, store, cache, metric, tags,
                startTime, endTime, delay, lag, request, ctx, scannerBatchSize, flushIntervalSeconds, scannerReadAhead,
                subscriptionBatchSize);
        METRICS.put(metric, m);
        m.start();
    }

    void scannerComplete(String metric) {
        deadMetricScannerQueue.add(metric);
    }

    private void cleanupCompletedMetrics() {
        String metric = this.deadMetricScannerQueue.poll();
        while (null != metric) {
            this.removeMetric(metric);
            metric = this.deadMetricScannerQueue.poll();
        }
    }

    public void removeMetric(String metric) {
        MetricScanner m = METRICS.remove(metric);
        if (null != m) {
            m.close();
            try {
                m.join();
            } catch (InterruptedException e) {
                LOG.error("[" + this.subscriptionId + "] Error closing MetricScanner", e);
            }
        }
    }

    public void close() {
        LOG.info("[{}] Closing subscriptions", this.subscriptionId);
        this.ping.cancel(false);
        METRICS.forEach((k, v) -> {
            v.close();
            try {
                v.join();
            } catch (InterruptedException e) {
                LOG.error("[" + this.subscriptionId + "] Error closing MetricScanner", e);
            }
        });
        METRICS.clear();
    }

}

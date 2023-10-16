package timely.subscription;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.ScheduledFuture;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.adapter.accumulo.MetricAdapter;
import timely.api.request.AuthenticatedRequest;
import timely.api.response.MetricResponse;
import timely.api.response.MetricResponses;
import timely.api.response.TimelyException;
import timely.model.Metric;
import timely.model.Tag;
import timely.store.DataStore;
import timely.store.cache.DataStoreCache;
import timely.util.JsonUtil;

public class MetricScanner extends Thread implements UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MetricScanner.class);
    private static final ObjectMapper om = JsonUtil.getObjectMapper();

    private final Scanner scanner;
    private Iterator<Entry<Key, Value>> iter = null;
    private final ChannelHandlerContext ctx;
    private final AuthenticatedRequest request;
    private volatile boolean closed = false;
    private final long delay;
    private String scannerInfo;
    private final int lag;
    private final String subscriptionId;
    private final String metric;
    private long beginTime;
    private long endTime;
    private final Subscription subscription;
    private final int subscriptionBatchSize;
    private MetricResponses responses = new MetricResponses();
    private ScheduledFuture<?> flusher = null;
    private List<Range> ranges = null;
    private Iterator<Range> rangeItr = null;
    private DataStore store = null;
    private DataStoreCache cache = null;
    private Set<Tag> colFamValues = null;
    private boolean completedResponseSent = false;
    private boolean done = false;

    public MetricScanner(Subscription sub, String subscriptionId, String sessionId, DataStore store,
            DataStoreCache cache, String metric, Map<String, String> tags, long beginTime, long endTime, long delay,
            int lag, AuthenticatedRequest request, ChannelHandlerContext ctx, int scannerBatchSize,
            int flushIntervalSeconds, int scannerReadAhead, int subscriptionBatchSize) throws TimelyException {
        this.setDaemon(true);
        this.setUncaughtExceptionHandler(this);
        this.store = store;
        this.cache = cache;
        this.subscription = sub;
        this.ctx = ctx;
        this.request = request;
        this.lag = lag;
        this.metric = metric;
        this.beginTime = beginTime;
        this.endTime = endTime;
        this.delay = delay;
        this.subscriptionId = subscriptionId;
        this.subscriptionBatchSize = subscriptionBatchSize;

        ToStringBuilder buf = new ToStringBuilder(this, ToStringStyle.JSON_STYLE);
        buf.append("sessionId", sessionId);
        buf.append("subscriptionId", subscriptionId);
        buf.append("metric", metric);
        buf.append("startTime", beginTime);
        buf.append("endTime", endTime);
        buf.append("delayTime", delay);
        if (null != tags) {
            buf.append("tags", tags.toString());
        }
        scannerInfo = buf.toString();
        LOG.info("[{}] Setting up MetricScanner: {}", subscriptionId, scannerInfo);

        if (0 == beginTime) {
            this.beginTime = (System.currentTimeMillis() - this.store.getAgeOffForMetric(metric) - 1000);
            LOG.debug("[{}] Overriding zero start time to {} due to age off configuration", subscriptionId, beginTime);
        }
        long endTimeStamp = (endTime == 0) ? (System.currentTimeMillis() - (lag * 1000)) : endTime;

        try {
            this.colFamValues = this.store.getColumnFamilies(metric, tags);
            this.ranges = new ArrayList<>();
            if (this.cache == null) {
                LOG.debug("[{}] Cache not enabled, adding complete range", subscriptionId);
                ranges.addAll(this.store.getQueryRanges(metric, this.beginTime, endTimeStamp, colFamValues));
            } else {
                long oldestTsForMetric = this.cache.getOldestTimestamp(metric);
                // metric is cached on this server and requested range ends somewhere in the
                // cache
                if (oldestTsForMetric < Long.MAX_VALUE && endTimeStamp > oldestTsForMetric) {
                    List<MetricResponse> metricsFromCache;
                    if (this.beginTime < oldestTsForMetric) {
                        // add range this.beginTime to oldestTsForMetric - 1 because this range is not
                        // cached
                        ranges.addAll(
                                this.store.getQueryRanges(metric, this.beginTime, oldestTsForMetric - 1, colFamValues));
                        metricsFromCache = cache.getMetricsFromCache(this.request, metric, tags, oldestTsForMetric,
                                endTimeStamp);
                        LOG.debug("[{}] MetricScanner request partially fulfilled from cache: {} metrics",
                                subscriptionId, metricsFromCache.size());
                    } else {
                        metricsFromCache = cache.getMetricsFromCache(this.request, metric, tags, this.beginTime,
                                endTimeStamp);
                        LOG.debug("[{}] Websocket request completely fulfilled from cache: {} metrics [{}]",
                                subscriptionId, metricsFromCache.size());
                    }

                    if (metricsFromCache.isEmpty()) {
                        // no metrics from cache, clear ranges and add whole range
                        ranges.clear();
                        ranges.addAll(this.store.getQueryRanges(metric, this.beginTime, endTimeStamp, colFamValues));
                        LOG.debug("[{}] MetricScanner request got no metrics from cache, re-adding complete range",
                                subscriptionId);
                    } else {
                        for (MetricResponse r : metricsFromCache) {
                            responses.addResponse(r);
                        }
                        flush();
                    }
                } else {
                    LOG.debug("[{}] MetricScanner request range not cached, adding complete range", subscriptionId);
                    ranges.addAll(this.store.getQueryRanges(metric, this.beginTime, endTimeStamp, colFamValues));
                }
            }

            if (ranges.isEmpty() && endTime == 0) {
                // request fulfilled from cache up until current, but user asked for continuous
                // data
                // must add a small range otherwise we will be scanning all data regardless of
                // metric, tag
                ranges.addAll(this.store.getQueryRanges(metric, endTimeStamp, endTimeStamp + 1, colFamValues));
            }

            if (ranges.isEmpty()) {
                this.scanner = null;
                this.done = true;
            } else {
                this.scanner = this.store.createScannerForMetric(this.request, metric, tags, scannerBatchSize,
                        scannerReadAhead);
                rangeItr = ranges.iterator();
                if (rangeItr.hasNext()) {
                    Range r = rangeItr.next();
                    this.scanner.setRange(r);
                }
            }
        } catch (TableNotFoundException e) {
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(),
                    "[" + subscriptionId + "] Error in MetricScanner", e.getMessage(), e);
        }

        if (!done) {
            this.iter = scanner.iterator();
            this.setName("Scan:" + subscriptionId);
            this.flusher = this.ctx.executor().scheduleAtFixedRate(() -> {
                flush();
            }, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
        }
    }

    public synchronized void flush() {
        LOG.debug("[{}] Flush called", subscriptionId);
        synchronized (responses) {
            if (responses.size() > 0) {
                try {
                    String json = om.writeValueAsString(responses);
                    this.ctx.writeAndFlush(new TextWebSocketFrame(json));
                    responses.clear();
                } catch (JsonProcessingException e) {
                    LOG.error("[" + subscriptionId + "] Error serializing metrics: " + responses, e);
                }
            }
        }
    }

    @Override
    public void run() {
        Metric m = null;
        try {
            while (!done && !closed) {

                if (this.iter.hasNext()) {
                    Entry<Key, Value> e = this.iter.next();
                    try {
                        m = MetricAdapter.parse(e.getKey(), e.getValue(), true);
                        if (responses.size() >= this.subscriptionBatchSize) {
                            flush();
                        }
                        this.responses.addResponse(MetricResponse.fromMetric(m, this.subscriptionId));
                    } catch (Exception e1) {
                        LOG.error("[{}] Error {} parsing metric at key: {}", subscriptionId, e1.getMessage(),
                                e.getKey().toString());
                    }
                } else if (rangeItr.hasNext()) {
                    // set next range on the scanner
                    Range r = rangeItr.next();
                    this.scanner.setRange(r);
                    this.iter = scanner.iterator();
                } else if (this.endTime == 0) {
                    flush();
                    long endTimeStamp = (System.currentTimeMillis() - (lag * 1000));
                    if (null == m) {
                        LOG.debug("[{}] No results found, waiting {}ms to retry with new end time {}. [{}]",
                                subscriptionId, delay, endTimeStamp);
                        sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
                        ranges = this.store.getQueryRanges(metric, beginTime, endTimeStamp, colFamValues);
                        rangeItr = ranges.iterator();
                        if (rangeItr.hasNext()) {
                            Range r = rangeItr.next();
                            this.scanner.setRange(r);
                            this.iter = scanner.iterator();
                        }
                    } else {
                        // Reset the starting range to the last key returned
                        LOG.debug("[{}] Exhausted scanner, last metric returned was {} [{}]", subscriptionId, m);
                        LOG.debug("[{}] Waiting {}ms to retry with new end time {}. [{}]", subscriptionId, delay,
                                endTimeStamp);
                        sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
                        ranges = this.store.getQueryRanges(metric, m.getValue().getTimestamp() + 1, endTimeStamp,
                                colFamValues);
                        rangeItr = ranges.iterator();
                        if (rangeItr.hasNext()) {
                            Range r = rangeItr.next();
                            this.scanner.setRange(r);
                            this.iter = this.scanner.iterator();
                        } else {
                            done = true;
                        }
                    }
                } else {
                    done = true;
                }
                if (done) {
                    LOG.debug("[{}] Exhausted scanner", subscriptionId);
                    sendCompletedResponse();
                }
            }
        } catch (Throwable e) {
            LOG.error("[" + subscriptionId + "] Error in metric scanner, closing.", e);
        } finally {
            close();
            if (this.scanner != null) {
                this.scanner.close();
            }
            subscription.scannerComplete(metric);
        }
    }

    synchronized private void sendCompletedResponse() {
        if (!completedResponseSent) {
            LOG.info("[{}] Sending completed response", subscriptionId);
            final MetricResponse completedResponse = new MetricResponse();
            completedResponse.setSubscriptionId(this.subscriptionId);
            completedResponse.setComplete(true);
            this.responses.addResponse(completedResponse);
            flush();
            completedResponseSent = true;
        }
    }

    public void close() {
        if (!closed) {
            LOG.debug("[{}] Marking metric scanner closed", subscriptionId);
            sendCompletedResponse();
            if (this.flusher != null) {
                this.flusher.cancel(false);
            }
            this.closed = true;
        }
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error("[" + subscriptionId + "] Error during metric scanner", e);
        this.close();
    }
}

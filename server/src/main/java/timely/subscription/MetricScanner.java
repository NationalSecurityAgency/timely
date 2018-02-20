package timely.subscription;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.ScheduledFuture;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.adapter.accumulo.MetricAdapter;
import timely.api.response.MetricResponse;
import timely.api.response.MetricResponses;
import timely.api.response.TimelyException;
import timely.model.Metric;
import timely.model.Tag;
import timely.store.DataStore;
import timely.util.JsonUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class MetricScanner extends Thread implements UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(MetricScanner.class);
    private static final ObjectMapper om = JsonUtil.getObjectMapper();

    private final Scanner scanner;
    private Iterator<Entry<Key, Value>> iter = null;
    private final ChannelHandlerContext ctx;
    private volatile boolean closed = false;
    private final long delay;
    private final String name;
    private final int lag;
    private final String subscriptionId;
    private final String metric;
    private long startTime;
    private long endTime;
    private final Subscription subscription;
    private MetricResponses responses = new MetricResponses();
    private final ScheduledFuture<?> flusher;
    private final int subscriptionBatchSize;
    private List<Range> ranges = null;
    private Iterator<Range> rangeItr = null;
    private DataStore store = null;
    private Set<Tag> colFamValues = null;

    public MetricScanner(Subscription sub, String subscriptionId, String sessionId, DataStore store, String metric,
            Map<String, String> tags, long startTime, long endTime, long delay, int lag, ChannelHandlerContext ctx,
            int scannerBatchSize, int flushIntervalSeconds, int scannerReadAhead, int subscriptionBatchSize)
            throws TimelyException {
        this.setDaemon(true);
        this.setUncaughtExceptionHandler(this);
        this.store = store;
        this.subscription = sub;
        this.ctx = ctx;
        this.lag = lag;
        this.metric = metric;
        this.startTime = startTime;
        this.endTime = endTime;
        this.scanner = this.store.createScannerForMetric(sessionId, metric, tags, startTime, endTime, lag,
                scannerBatchSize, scannerReadAhead);

        if (0 == startTime) {
            this.startTime = (System.currentTimeMillis() - this.store.getAgeOffForMetric(metric) - 1000);
            LOG.debug("Overriding zero start time to {} due to age off configuration", startTime);
        }
        long endTimeStamp = (endTime == 0) ? (System.currentTimeMillis() - (lag * 1000)) : endTime;

        try {
            this.colFamValues = this.store.getColumnFamilies(metric, tags);
            ranges = this.store.getQueryRanges(metric, this.startTime, endTimeStamp, colFamValues);
            rangeItr = ranges.iterator();
            if (rangeItr.hasNext()) {
                Range r = rangeItr.next();
                this.scanner.setRange(r);
            }
        } catch (TableNotFoundException e) {
            throw new TimelyException(HttpResponseStatus.INTERNAL_SERVER_ERROR.code(), "Error in MetricScanner",
                    e.getMessage(), e);
        }
        this.iter = scanner.iterator();
        this.delay = delay;
        this.subscriptionId = subscriptionId;
        this.subscriptionBatchSize = subscriptionBatchSize;
        ToStringBuilder buf = new ToStringBuilder(this);
        buf.append("sessionId", sessionId);
        buf.append("metric", metric);
        buf.append("startTime", startTime);
        buf.append("endTime", endTime);
        buf.append("delayTime", delay);
        if (null != tags) {
            buf.append("tags", tags.toString());
        }
        name = buf.toString();
        this.setName("Metric Scanner " + name);
        this.flusher = this.ctx.executor().scheduleAtFixedRate(() -> {
            flush();
        }, flushIntervalSeconds, flushIntervalSeconds, TimeUnit.SECONDS);
        LOG.trace("Created MetricScanner: {}", name);
    }

    public synchronized void flush() {
        LOG.info("Flush called");
        synchronized (responses) {
            if (responses.size() > 0) {
                try {
                    String json = om.writeValueAsString(responses);
                    this.ctx.writeAndFlush(new TextWebSocketFrame(json));
                    responses.clear();
                } catch (JsonProcessingException e) {
                    LOG.error("Error serializing metrics: " + responses, e);
                }
            }
        }
    }

    @Override
    public void run() {
        Metric m = null;
        try {
            boolean done = false;
            while (!done && !closed) {

                if (this.iter.hasNext()) {
                    Entry<Key, Value> e = this.iter.next();
                    m = MetricAdapter.parse(e.getKey(), e.getValue(), true);
                    if (responses.size() >= this.subscriptionBatchSize) {
                        flush();
                    }
                    this.responses.addResponse(MetricResponse.fromMetric(m, this.subscriptionId));
                } else if (rangeItr.hasNext()) {
                    // set next range on the scanner
                    Range r = rangeItr.next();
                    this.scanner.close();
                    this.scanner.setRange(r);
                    this.iter = scanner.iterator();
                } else if (this.endTime == 0) {
                    flush();
                    long endTimeStamp = (System.currentTimeMillis() - (lag * 1000));
                    this.scanner.close();
                    if (null == m) {
                        LOG.debug("No results found, waiting {}ms to retry with new end time {}.", delay, endTimeStamp);
                        sleepUninterruptibly(delay, TimeUnit.MILLISECONDS);
                        ranges = this.store.getQueryRanges(metric, startTime, endTimeStamp, colFamValues);
                        rangeItr = ranges.iterator();
                        if (rangeItr.hasNext()) {
                            Range r = rangeItr.next();
                            this.scanner.setRange(r);
                            this.iter = scanner.iterator();
                        }
                    } else {
                        // Reset the starting range to the last key returned
                        LOG.debug("Exhausted scanner, last metric returned was {}", m);
                        LOG.debug("Waiting {}ms to retry with new end time {}.", delay, endTimeStamp);
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
                    LOG.debug("Exhausted scanner, sending completed message for subscription {}", this.subscriptionId);
                    final MetricResponse last = new MetricResponse();
                    last.setSubscriptionId(this.subscriptionId);
                    last.setComplete(true);
                    this.responses.addResponse(last);
                    flush();
                }
            }
        } catch (Throwable e) {
            LOG.error("Error in metric scanner, closing.", e);
        } finally {
            close();
            this.scanner.close();
            subscription.scannerComplete(metric);
        }
    }

    public void close() {
        if (!closed) {
            LOG.info("Marking metric scanner closed: {}", name);
            flush();
            this.flusher.cancel(false);
            this.closed = true;
        }
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        LOG.error("Error during metric scanner " + name, e);
        this.close();
    }
}

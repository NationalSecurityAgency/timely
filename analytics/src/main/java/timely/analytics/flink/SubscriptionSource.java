package timely.analytics.flink;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.response.MetricResponse;
import timely.api.response.MetricResponses;
import timely.client.websocket.ClientHandler;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;
import timely.serialize.JsonSerializer;

public class SubscriptionSource extends RichSourceFunction<MetricResponse> implements Serializable, StoppableFunction {

    private static class SubscriptionThreadFactory implements ThreadFactory {

        private static final String NAME = "Subscription Source";
        private AtomicInteger threadNum = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, NAME + " " + threadNum.getAndIncrement());
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSource.class);
    private static final long serialVersionUID = 1L;

    private transient WebSocketSubscriptionClient client = null;
    private long start = 0L;
    private long end = 0L;
    private final String[] metrics;
    private final SummarizationJobParameters jp;
    private final long window;
    private final AtomicInteger subscriptionsRemaining;

    public SubscriptionSource(SummarizationJobParameters jp) {
        this.jp = jp;
        start = jp.getStartTime();
        end = jp.getEndTime();
        metrics = jp.getMetrics();
        window = jp.getSummarizationInterval().toMilliseconds();
        subscriptionsRemaining = new AtomicInteger(metrics.length);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        LOG.info("Opening summarization job.");
        super.open(parameters);
        client = new WebSocketSubscriptionClient(jp.getTimelyHostname(), jp.getTimelyHttpsPort(),
                jp.getTimelyWssPort(), jp.isDoLogin(), jp.getTimelyUsername(), jp.getTimelyPassword(),
                jp.getKeyStoreFile(), jp.getKeyStoreType(), jp.getKeyStorePass(), jp.getTrustStoreFile(),
                jp.getTrustStoreType(), jp.getTrustStorePass(), jp.isHostVerificationEnabled(), jp.getBufferSize());
    }

    @Override
    public synchronized void close() throws Exception {
        super.close();
        if (null != client) {
            client.close();
        }
    }

    @Override
    public void run(SourceContext<MetricResponse> ctx) throws Exception {
        LOG.info("Running summarization job");
        final SortedStringAccumulator dateTimeAccumulator = new SortedStringAccumulator();
        this.getRuntimeContext().addAccumulator("source hourly count", dateTimeAccumulator);
        final LongCounter sourceInputs = new LongCounter();
        this.getRuntimeContext().addAccumulator("source input counter", sourceInputs);
        final LongCounter inputProcessedInMainThread = new LongCounter();
        this.getRuntimeContext().addAccumulator("input processed in main thread", inputProcessedInMainThread);

        // Use only one thread to ensure that time is still ordered
        final ExecutorService svc = Executors.newFixedThreadPool(1, new SubscriptionThreadFactory());

        ClientHandler handler = new ClientHandler() {

            @Override
            public void onOpen(Session session, EndpointConfig config) {
                session.addMessageHandler(new MessageHandler.Whole<String>() {

                    private final SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HH");
                    private long lastWatermarkTime = 0L;

                    @Override
                    public void onMessage(final String message) {
                        LOG.info("Message received on Websocket session {}, length: {}", session.getId(),
                                message.length());
                        try {
                            // Deserialize in this thread
                            final MetricResponses responses = JsonSerializer.getObjectMapper().readValue(message,
                                    MetricResponses.class);
                            // Process in the other
                            final Runnable r = () -> {
                                responses.getResponses().forEach(response -> {
                                    if (response.isComplete()) {
                                        LOG.info("Received last message.");
                                        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
                                        subscriptionsRemaining.getAndDecrement();
                                        return;
                                    }
                                    LOG.trace("Received metric: {}", response);
                                    long time = response.getTimestamp();
                                    ctx.collectWithTimestamp(response, time);
                                    dateTimeAccumulator.add(formatter.format(new Date(response.getTimestamp())));
                                    sourceInputs.add(1);
                                    // Emit a watermark every second of event
                                    // time
                                        if (lastWatermarkTime == 0) {
                                            lastWatermarkTime = time;
                                        } else if ((time - lastWatermarkTime) > window) {
                                            lastWatermarkTime = time;
                                            ctx.emitWatermark(new Watermark(time - 1));
                                        }
                                    });
                            };
                            try {
                                svc.execute(r);
                            } catch (RejectedExecutionException e) {
                                // Run it in this thread
                                r.run();
                                inputProcessedInMainThread.add(1);
                            }
                        } catch (IOException e) {
                            LOG.error("Error deserializing metric response, closing", e);
                            try {
                                close();
                            } catch (Exception e1) {
                                LOG.error("Error closing client", e1);
                            }
                        }
                    }
                });
            }

            @Override
            public void onClose(Session session, CloseReason reason) {
                super.onClose(session, reason);
                try {
                    close();
                } catch (Exception e1) {
                    LOG.error("Error closing client", e1);
                }
                // Signal done sending data
                ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void onError(Session session, Throwable error) {
                super.onError(session, error);
                try {
                    close();
                } catch (Exception e1) {
                    LOG.error("Error closing client", e1);
                }
            }

        };
        try {
            client.open(handler);
            for (String m : metrics) {
                LOG.info("Adding subscription for {}", m);
                client.addSubscription(m, null, start, end, 5000);
            }
            while (!client.isClosed() && subscriptionsRemaining.get() > 0) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    LOG.error("Sleep interrupted.");
                }
            }
        } finally {
            svc.shutdown();
        }
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling subscription source.");
        try {
            close();
        } catch (Exception e) {
            LOG.error("Error closing web socket client");
        }
    }

    @Override
    public void stop() {
        LOG.info("Stopping subscription source.");
        try {
            close();
        } catch (Exception e) {
            LOG.error("Error closing web socket client");
        }

    }

}

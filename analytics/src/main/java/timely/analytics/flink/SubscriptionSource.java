package timely.analytics.flink;

import java.io.IOException;
import java.io.Serializable;

import javax.websocket.CloseReason;
import javax.websocket.EndpointConfig;
import javax.websocket.MessageHandler;
import javax.websocket.Session;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.response.MetricResponse;
import timely.client.websocket.ClientHandler;
import timely.client.websocket.subscription.WebSocketSubscriptionClient;
import timely.serialize.JsonSerializer;

public class SubscriptionSource extends RichSourceFunction<MetricResponse> implements Serializable, StoppableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionSource.class);
    private static final long serialVersionUID = 1L;

    private transient WebSocketSubscriptionClient client = null;
    private long start = 0L;
    private long end = 0L;
    private final String[] metrics;
    private final SummarizationJobParameters jp;

    public SubscriptionSource(SummarizationJobParameters jp) {
        this.jp = jp;
        start = jp.getStartTime();
        end = jp.getEndTime();
        metrics = jp.getMetrics();
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
        ClientHandler handler = new ClientHandler() {

            @Override
            public void onOpen(Session session, EndpointConfig config) {
                session.addMessageHandler(new MessageHandler.Whole<String>() {

                    @Override
                    public void onMessage(String message) {
                        MetricResponse response;
                        try {
                            response = JsonSerializer.getObjectMapper().readValue(message, MetricResponse.class);
                            LOG.trace("Sending response: {}", response);
                            ctx.collectWithTimestamp(response, response.getTimestamp());
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
        client.open(handler);
        for (String m : metrics) {
            LOG.info("Adding subscription for {}", m);
            client.addSubscription(m, null, start, end, 5000);
        }
        while (!client.isClosed()) {
            Thread.sleep(5000);
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

package timely.nsq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import timely.client.tcp.TcpClient;
import timely.model.Metric;
import timely.model.parse.MetricParser;
import timely.nsq.config.TimelyClientProperties;

public class TimelyNsqRelayHandler {
    private static final Logger log = LoggerFactory.getLogger(TimelyNsqRelayHandler.class);
    private final TimelyClientProperties timelyClientProperties;
    private final LinkedBlockingQueue<String> messageQueue;
    private final ExecutorService relayExecutor;
    private final List<Future> relayRunnableFutures = new ArrayList<>();

    private final ThreadLocal<TcpClient> tcpClient;
    private final Map<String,TcpClient> tcpClientMap = Collections.synchronizedMap(new HashMap<>());

    public TimelyNsqRelayHandler(TimelyClientProperties timelyClientProperties, LinkedBlockingQueue<String> messageQueue) {
        this.timelyClientProperties = timelyClientProperties;
        this.messageQueue = messageQueue;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("relay-handler-%d").build();
        this.relayExecutor = Executors.newFixedThreadPool(timelyClientProperties.getRelayHandlerTheads(), threadFactory);

        this.tcpClient = new ThreadLocal<>() {
            @Override
            protected TcpClient initialValue() {
                TcpClient client = new TcpClient(timelyClientProperties.getHost(), timelyClientProperties.getTcpPort(),
                                timelyClientProperties.getWritesToBuffer(), timelyClientProperties.getMaxLatencyMs());
                tcpClientMap.put(Thread.currentThread().getName(), client);
                return client;
            }

            @Override
            public TcpClient get() {
                TcpClient client = super.get();
                long now = System.currentTimeMillis();
                long clientTimeToLive = timelyClientProperties.getTimeToLiveMs();
                if (null != client && (clientTimeToLive > -1) && (now > client.getConnectTime() + clientTimeToLive)) {
                    try {
                        client.close();
                    } catch (Exception e) {
                        log.error("Failed to close client:" + e.getMessage(), e);
                    }
                    client = new TcpClient(timelyClientProperties.getHost(), timelyClientProperties.getTcpPort(), -1);
                    set(client);
                    tcpClientMap.put(Thread.currentThread().getName(), client);
                }
                return client;
            }
        };

        for (int i = 0; i < timelyClientProperties.getRelayHandlerTheads(); i++) {
            relayRunnableFutures.add(this.relayExecutor.submit(new RelayRunnable()));
        }
    }

    public void shutdown() {
        this.relayExecutor.shutdown();
        this.relayRunnableFutures.forEach(f -> {
            try {
                f.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                f.cancel(true);
            }
        });
        synchronized (this.tcpClientMap) {
            for (Map.Entry<String,TcpClient> entry : this.tcpClientMap.entrySet()) {
                TcpClient client = entry.getValue();
                try {
                    log.info("Closing timely client for thread: " + entry.getKey());
                    client.close();
                } catch (Exception e) {
                    log.error("Failed to close client for thread: " + entry.getKey() + ": " + e.getMessage(), e);
                }
            }
        }
    }

    private String toTcpFormat(String metricMessage) {
        MetricParser metricParser = new MetricParser();
        Metric metric = metricParser.parse(metricMessage);
        StringBuilder sb = new StringBuilder();
        sb.append("put ");
        sb.append(metric.getName());
        sb.append(" ");
        sb.append(metric.getValue().getTimestamp());
        sb.append(" ");
        sb.append(metric.getValue().getMeasure());
        metric.getTags().stream().forEach(tag -> sb.append(" ").append(tag.getKey()).append("=").append(tag.getValue()));
        return sb.toString();
    }

    private class RelayRunnable implements Runnable {

        private boolean shutdown = false;

        @Override
        public void run() {
            while (!shutdown) {
                TcpClient client = tcpClient.get();
                // try to avoid polling the queue if the client is not connected
                if (client.isConnected()) {
                    try {
                        String metricMessage = messageQueue.poll(1, TimeUnit.SECONDS);
                        String metricString = null;
                        if (metricMessage != null) {
                            try {
                                metricString = toTcpFormat(metricMessage);
                            } catch (Exception e) {
                                log.error("Failed to parse metric: ", metricMessage);
                            }
                        }
                        while (!shutdown && metricString != null) {
                            try {
                                if (client.isConnected()) {
                                    log.trace("Writing metric:{} queueSize:{}", metricMessage, messageQueue.size());
                                    client.write(metricString + "\n");
                                    metricString = null;
                                }
                            } catch (Exception e) {
                                sleep(1000);
                            }
                        }
                    } catch (InterruptedException e) {
                        sleep(100);
                    }
                } else {
                    log.trace("Timely client not connected");
                    sleep(1000);
                }
            }
        }

        private void sleep(long millis) {
            try {
                Thread.sleep(millis);
            } catch (InterruptedException e) {

            }
        }

        public void shutdown() {
            shutdown = true;
        }
    }
}

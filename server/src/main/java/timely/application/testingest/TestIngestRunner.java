package timely.application.testingest;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PreDestroy;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import timely.client.tcp.TcpClient;
import timely.server.configuration.LoadTestProperties;

@Component
@EnableConfigurationProperties(LoadTestProperties.class)
public class TestIngestRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(TestIngestRunner.class);
    private ApplicationContext context;
    private LoadTestProperties loadTestProperties;
    private long samplePeriodMs;
    private List<MetricPut> metrics = new ArrayList<>();
    private Queue<MetricPut> metricsToPut = new LinkedList<>();
    private long nextTimestamp;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
    private ExecutorService executorService;

    private Runnable task = () -> {
        ThreadLocalTcpClient client = new ThreadLocalTcpClient(loadTestProperties);
        long ts;
        while (true) {
            MetricPut metricPut = getNextMetric();
            ts = getNextTimestamp();
            // once we have caught up, don't post metrics until ts >= current timestamp
            while (ts > System.currentTimeMillis()) {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}
            }
            try {
                try {
                    String line = metricPut.generateTcpLine(ts);
                    System.out.println(line);
                    client.get().write(line);
                    client.get().flush();
                } catch (IOException e) {
                    log.error(e.getMessage(), e);
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    };

    public TestIngestRunner(ApplicationContext context, LoadTestProperties loadTestProperties) {
        this.context = context;
        this.loadTestProperties = loadTestProperties;
    }

    @PreDestroy
    public void shutdown() {
        if (this.executorService != null) {
            this.executorService.shutdownNow();
        }
    }

    private void readMetrics() {
        String fileLoc = loadTestProperties.getTestDataFile();
        log.debug("Using metricTestData from: " + fileLoc);
        File testDataFile = new File(fileLoc);
        try {
            readMetricsFromFile(testDataFile);
            metricsToPut.addAll(metrics);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readMetricsFromFile(File file) throws IOException {

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        for (String l : lines) {
            String lineSplit[] = l.split(",");
            if (lineSplit.length >= 2) {
                String metric = lineSplit[0];
                Map<String,String> tags = new HashMap<>();
                String tagSplitOnSpaces[] = lineSplit[1].split(" ");
                for (String tagAndValue : tagSplitOnSpaces) {
                    String tagSplit[] = tagAndValue.split("=");
                    tags.put(tagSplit[0], tagSplit[1]);
                }
                metrics.add(new MetricPut(metric, tags));
            }
        }
    }

    public long getNextTimestamp() {
        long ts;
        synchronized (metricsToPut) {
            ts = nextTimestamp;
        }
        return ts;
    }

    public MetricPut getNextMetric() {
        MetricPut metricPut;
        synchronized (metricsToPut) {
            if (metricsToPut.isEmpty()) {
                metricsToPut.addAll(metrics);
                nextTimestamp = nextTimestamp + samplePeriodMs;
                System.out.println("Sending metrics for ts=" + nextTimestamp + "(" + sdf.format(new Date(nextTimestamp)) + ")");
            }
            metricPut = metricsToPut.remove();
        }
        return metricPut;
    }

    private void writeMetrics() {
        for (int x = 0; x < loadTestProperties.getNumWriteThreads(); x++) {
            executorService.execute(task);
        }
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws Exception {
        try {
            long minutesBacklog = loadTestProperties.getBacklogMinutes();
            log.debug("Generating " + minutesBacklog + " minutes of backlog metrics");
            this.nextTimestamp = System.currentTimeMillis() - minutesBacklog * 60 * 1000;
            this.samplePeriodMs = loadTestProperties.getSamplePeriodMs();
            this.executorService = Executors.newFixedThreadPool(loadTestProperties.getNumWriteThreads());

            readMetrics();
            writeMetrics();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            SpringApplication.exit(context);
        }
    }

    static private class ThreadLocalTcpClient extends ThreadLocal<TcpClient> {

        private String host;
        private int port;

        public ThreadLocalTcpClient(LoadTestProperties loadTestProperties) {
            this.host = loadTestProperties.getHost();
            this.port = loadTestProperties.getTcpPort();
        }

        @Override
        protected TcpClient initialValue() {
            TcpClient client = new TcpClient(host, port);
            try {
                client.open();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
            return client;
        }

        @Override
        public void remove() {
            try {
                get().close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}

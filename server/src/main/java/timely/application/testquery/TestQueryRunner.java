package timely.application.testquery;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PreDestroy;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import timely.common.configuration.SslClientProperties;
import timely.server.configuration.LoadTestProperties;

@Component
public class TestQueryRunner implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(TestQueryRunner.class);
    private ApplicationContext context;
    private LoadTestProperties loadTestProperties;
    private SslClientProperties sslClientProperties;
    private ExecutorService executorService = null;
    private List<String> metrics = new ArrayList<>();
    private Queue<String> metricsToQuery = new LinkedList<>();
    private boolean testDone = false;
    private AtomicLong totalQueryDuration = new AtomicLong(0);
    private AtomicLong totalQueriesCompleted = new AtomicLong(0);

    private Runnable task = () -> {
        try {
            long now = System.currentTimeMillis();
            long begin = now - (1000 * loadTestProperties.getQueryPeriodMinutes() * 60);
            String metric = getNextMetric();
            Map<String,String> tags = new HashMap<>();
            tags.put("host", ".*");
            MetricQuery q = new MetricQuery(metric, begin, now, tags);

            TimelyHttpsUtil connection = new TimelyHttpsUtil(sslClientProperties);

            long start = System.currentTimeMillis();
            CloseableHttpResponse response = connection.query(q, loadTestProperties.getHost(), loadTestProperties.getHttpPort());
            long stop = System.currentTimeMillis();
            long duration = stop - start;
            if (!testDone) {
                log.debug("Query for " + metric + " response: " + response.getStatusLine() + " in " + duration + "ms");
                totalQueryDuration.addAndGet(duration);
                totalQueriesCompleted.incrementAndGet();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    };

    public TestQueryRunner(ApplicationContext context, LoadTestProperties loadTestProperties, SslClientProperties sslClientProperties) {
        this.context = context;
        this.loadTestProperties = loadTestProperties;
        this.sslClientProperties = sslClientProperties;
    }

    @PreDestroy
    public void shutdown() {
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    private void readMetricsFromFile(File file) throws IOException {
        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
        for (String l : lines) {
            String lineSplit[] = l.split(",");
            if (lineSplit.length >= 1) {
                metrics.add(lineSplit[0]);
            }
        }
    }

    public String getNextMetric() {
        String metric;
        synchronized (metricsToQuery) {
            if (metricsToQuery.isEmpty()) {
                metricsToQuery.addAll(metrics);
            }
            metric = metricsToQuery.remove();
        }
        return metric;
    }

    public void run(ApplicationArguments args) throws Exception {
        try {
            log.info("Using metricTestData from: " + loadTestProperties.getTestDataFile());
            long queryPeriodMinutes = loadTestProperties.getQueryPeriodMinutes();
            System.out.println("Generating queries of " + queryPeriodMinutes + " minutes duration");

            readMetricsFromFile(new File(loadTestProperties.getTestDataFile()));
            executorService = Executors.newFixedThreadPool(loadTestProperties.getNumQueryThreads());
            long beginQueries = System.currentTimeMillis();

            long numQueries = 1000;
            long queriesCompleted = 0;
            List<Future> futures = new ArrayList<>();

            while (!testDone) {
                while (futures.size() < numQueries) {
                    futures.add(executorService.submit(task));
                }
                Iterator<Future> itr = futures.iterator();
                while (itr.hasNext()) {
                    Future f = itr.next();
                    if (f.isDone()) {
                        itr.remove();
                        queriesCompleted++;
                    }
                }
                Thread.sleep(100);
                testDone = (System.currentTimeMillis() - beginQueries) >= (loadTestProperties.getTestDurationMins() * 60 * 1000);
            }
            log.info("All Done");
            long endQueries = System.currentTimeMillis();
            long asSeenTime = totalQueryDuration.get() / totalQueriesCompleted.get();
            log.info("Average individual rate for " + totalQueriesCompleted.get() + " = " + asSeenTime + "ms/query");
            double msPerQuery = (endQueries - beginQueries) / ((double) queriesCompleted);
            long numQueriesPerHour = Math.round((60 * 60 * 1000) / msPerQuery);
            log.info("Aggregate rate for " + queriesCompleted + " = " + msPerQuery + "ms/query");
            log.info("Queries per hour = " + numQueriesPerHour);
        } catch (RuntimeException e) {
            log.error(e.getMessage(), e);
            SpringApplication.exit(context);
        }
    }
}

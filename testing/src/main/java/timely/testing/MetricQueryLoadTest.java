package timely.testing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;

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

public class MetricQueryLoadTest {

    private ExecutorService executorService = null;
    private List<String> metrics = new ArrayList<>();
    private Queue<String> metricsToQuery = new LinkedList<>();
    private String trustStoreFile;
    private String trustStoreType;
    private String trustStorePass;
    private String keyStoreFile;
    private String keyStoreType;
    private String keyStorePass;
    private long queryDurationMins = 60;
    private long testDurationMins = 5;
    private int numThreads = 8;
    private String host = "localhost";
    private int port = 4243;
    private File testDataFile;
    private boolean testDone = false;
    private AtomicLong totalQueryDuration = new AtomicLong(0);
    private AtomicLong totalQueriesCompleted = new AtomicLong(0);


    public MetricQueryLoadTest() {

    }

    public void setTrustStoreFile(String trustStoreFile) {
        this.trustStoreFile = trustStoreFile;
    }

    public void setTrustStorePass(String trustStorePass) {
        this.trustStorePass = trustStorePass;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public void setKeyStoreFile(String keyStoreFile) {
        this.keyStoreFile = keyStoreFile;
    }

    public void setKeyStorePass(String keyStorePass) {
        this.keyStorePass = keyStorePass;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public void setNumThreads(int numThreads) {
        this.numThreads = numThreads;
    }

    public void setQueryDurationMins(long queryDurationMins) {
        this.queryDurationMins = queryDurationMins;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setTestDataFile(File testDataFile) {
        this.testDataFile = testDataFile;
    }

    public void setTestDurationMins(long testDurationMins) {
        this.testDurationMins = testDurationMins;
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

        String metric = null;
        synchronized (metricsToQuery) {

            if (metricsToQuery.isEmpty()) {
                metricsToQuery.addAll(metrics);
            }
            metric = metricsToQuery.remove();
        }
        return metric;
    }

    public void run() {

        Runnable runnableTask = () -> {
            try {
                long now = System.currentTimeMillis();
                long begin = now - (1000 * queryDurationMins * 60);
                String metric = getNextMetric();
                Map<String, String> tags = new HashMap<>();
                tags.put("host", ".*");
                MetricQuery q = new MetricQuery(metric, begin, now, tags);

                TimelyHttpsUtil connection = new TimelyHttpsUtil(trustStoreFile, trustStoreType, trustStorePass,
                        keyStoreFile, keyStoreType, keyStorePass);

                long start = System.currentTimeMillis();
                CloseableHttpResponse response = connection.query(q, host, port);
                long stop = System.currentTimeMillis();
                long duration = stop - start;
                if (!testDone) {
                    System.out.println("Query for " + metric + " response: " + response.getStatusLine() + " in " + duration + "ms");
                    totalQueryDuration.addAndGet(duration);
                    totalQueriesCompleted.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

        try {
            readMetricsFromFile(testDataFile);
            executorService = Executors.newFixedThreadPool(numThreads);
            long beginQueries = System.currentTimeMillis();

            long numQueries = 1000;
            long queriesCompleted = 0;
            List<Future> futures = new ArrayList<>();

            while (!testDone) {
                testDone = (System.currentTimeMillis() - beginQueries) >= (testDurationMins * 60 * 1000);
                if (testDone) {
                    executorService.shutdownNow();
                    executorService = null;
                } else {
                    while (futures.size() < numQueries) {
                        futures.add(executorService.submit(runnableTask));
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
                }
            }

            System.out.println("All Done");
            long endQueries = System.currentTimeMillis();


            long asSeenTime = totalQueryDuration.get() / totalQueriesCompleted.get();
            System.out.println("Average individual rate for " + totalQueriesCompleted.get() + " = " + asSeenTime + "ms/query");
            double msPerQuery = (endQueries - beginQueries) / queriesCompleted;
            long numQueriesPerHour = Math.round((60 * 60 * 1000) / msPerQuery);
            System.out.println("Aggregate rate for " + queriesCompleted + " = " + msPerQuery + "ms/query");
            System.out.println("Queries per hour = " + numQueriesPerHour);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (executorService != null) {
                executorService.shutdownNow();
            }
        }
    }

    public static void main(String[] args) {


        try {
            MetricQueryLoadTest lt = new MetricQueryLoadTest();
            String fileLoc;
            if (args.length >= 5) {
                String host = args[0];
                lt.setHost(host);
                int port = Integer.parseInt(args[1]);
                lt.setPort(port);
                fileLoc = MetricQueryLoadTest.class.getClassLoader().getResource(args[2]).getFile();
                File file = new File(fileLoc);
                lt.setTestDataFile(file);
                System.out.println("Using metricTestData from: " + fileLoc);

                String threadsStr = System.getenv("NUM_QUERY_THREADS");
                int threads = 8;
                if (StringUtils.isNotEmpty(threadsStr)) {
                    threads = Integer.parseInt(threadsStr);
                }
                lt.setNumThreads(threads);

                lt.setTrustStoreFile(System.getenv("TRUSTSTORE_FILE"));
                lt.setTrustStorePass(System.getenv("TRUSTSTORE_PASS"));
                lt.setTrustStoreType(System.getenv("TRUSTSTORE_TYPE"));
                lt.setKeyStoreFile(System.getenv("KEYSTORE_FILE"));
                lt.setKeyStorePass(System.getenv("KEYSTORE_PASS"));
                lt.setKeyStoreType(System.getenv("KEYSTORE_TYPE"));

                long queryDurationMinutes = Long.parseLong(args[3]);
                System.out.println("Generating queries of " + queryDurationMinutes + " minutes duration");
                lt.setQueryDurationMins(queryDurationMinutes);
                long testDuration = Long.parseLong(args[4]);
                lt.setTestDurationMins(testDuration);

                lt.run();

            } else {
                System.out.println("Usage " + MetricQueryLoadTest.class.getName() + " host port metricTestData queryDurationMins testDurationMins");
                System.exit(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

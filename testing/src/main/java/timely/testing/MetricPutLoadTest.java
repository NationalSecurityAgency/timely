package timely.testing;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import timely.client.tcp.TcpClient;

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
import java.util.concurrent.Future;

public class MetricPutLoadTest {

    private ExecutorService executorService = null;
    private long beginTs;
    private long samplePeriod;
    private List<MetricPut> metrics = new ArrayList<>();
    private Queue<MetricPut> metricsToPut = new LinkedList<>();
    private String host = "localhost";
    private int port = 4242;
    private long nextTimestamp;
    private int numThreads = 8;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");

    public MetricPutLoadTest(String host, int port, File testDataFile, int numThreads, long beginTs, long samplePeriod) {
        this.host = host;
        this.port = port;
        this.numThreads = numThreads;
        executorService = Executors.newFixedThreadPool(numThreads);
        this.beginTs = beginTs;
        this.samplePeriod = samplePeriod;
        try {
            readMetricsFromFile(testDataFile);
            metricsToPut.addAll(metrics);
        } catch (IOException e) {
            e.printStackTrace();
        }
        nextTimestamp = beginTs;
    }

    private void readMetricsFromFile(File file) throws IOException {

        List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());

        for (String l : lines) {
            String lineSplit[] = l.split(",");
            if (lineSplit.length >= 2) {
                String metric = lineSplit[0];
                Map<String, String> tags = new HashMap<>();
                String tagSplitOnSpaces[] = lineSplit[1].split(" ");
                for (String tagAndValue : tagSplitOnSpaces) {
                    String tagSplit[] = tagAndValue.split("=");
                    tags.put(tagSplit[0], tagSplit[1]);
                }
                System.out.println("Adding " + metric + " tags: " + tags.toString());
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

        MetricPut metricPut = null;
        synchronized (metricsToPut) {

            if (metricsToPut.isEmpty()) {
                metricsToPut.addAll(metrics);
                nextTimestamp = nextTimestamp + samplePeriod;
                System.out.println("Sending metrics for ts=" + nextTimestamp + "(" + sdf.format(new Date(nextTimestamp)) + ")");
            }
            metricPut = metricsToPut.remove();
        }
        return metricPut;
    }


    public void run() {

        Runnable runnableTask = () -> {
            ThreadLocalTcpClient client = new ThreadLocalTcpClient(host, port);
            long ts;
            while (true) {
                MetricPut metricPut = getNextMetric();
                ts = getNextTimestamp();
                // once we have caught up, don't post metrics until ts >= current timestamp
                while (ts > System.currentTimeMillis()) {
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {

                    }
                }
                try {

                    try {
                        String line = metricPut.generateTcpLine(ts);
                        System.out.println(line);
                        client.get().write(line);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        List<Future> futures = new ArrayList<>();
        for (int x=0; x < numThreads; x++) {
            futures.add(executorService.submit(runnableTask));
        }
    }

    public static void main(String[] args) {

        try {
            String fileLoc;
            if (args.length >= 4) {
                String host = args[0];
                int port = Integer.parseInt(args[1]);
                fileLoc = MetricPutLoadTest.class.getClassLoader().getResource(args[2]).getFile();
                System.out.println("Using metricTestData from: " + fileLoc);
                long minutesBacklog = Long.parseLong(args[3]);
                System.out.println("Generating " + minutesBacklog + " minutes of backlog metrics");
                File file = new File(fileLoc);
                long beginTs = System.currentTimeMillis() - minutesBacklog * 60 * 1000;
                String threadsStr = System.getenv("NUM_INGEST_THREADS");
                int threads = 8;
                if (StringUtils.isNotEmpty(threadsStr)) {
                    threads = Integer.parseInt(threadsStr);
                }
                MetricPutLoadTest lt = new MetricPutLoadTest(host, port, file, threads, beginTs, 60000);
                lt.run();
            } else {
                System.out.println("Usage " + MetricPutLoadTest.class.getName() + " host port metricTestData backlogMinutes");
                System.exit(-1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class ThreadLocalTcpClient extends ThreadLocal<TcpClient> {

        private String host;
        private int port;

        public ThreadLocalTcpClient(String host, int port) {
            this.host = host;
            this.port = port;
        }

        @Override
        protected TcpClient initialValue() {
            System.out.println("initialized threadlocal");
            TcpClient client = new TcpClient(host, port);
            try {
                client.open();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return client;
        }

        @Override
        public void remove() {
            try {
                get().close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

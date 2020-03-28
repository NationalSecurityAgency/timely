package timely.balancer.resolver;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.balancer.configuration.BalancerConfiguration;
import timely.balancer.connection.TimelyBalancedHost;
import timely.balancer.resolver.eventing.MetricAssignedEvent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static timely.balancer.Balancer.ASSIGNMENTS_LAST_UPDATED_PATH;

public class MetricAssignmentPerister {

    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private enum PeristerType {
        HDFS, FILE
    }

    // passed in through constructor
    BalancedMetricResolver balancedMetricResolver;
    private BalancerConfiguration balancerConfig;
    private Map<String, TimelyBalancedHost> metricToHostMap;
    private Set<String> nonCachedMetrics;
    private InterProcessReadWriteLock assignmentsIPRWLock;
    private ReentrantReadWriteLock nonCachedMetricsLocalLock;
    private ReentrantReadWriteLock balancerLock;
    private AtomicLong assignmentsLastUpdatedLocal;

    // local
    private DistributedAtomicLong assignmentsLastUpdatedInHdfs;

    public MetricAssignmentPerister(BalancedMetricResolver balancedMetricResolver,
                                        BalancerConfiguration balancerConfig,
                                        Map<String, TimelyBalancedHost> metricToHostMap, Set<String> nonCachedMetrics,
                                        CuratorFramework curatorFramework,
                                        InterProcessReadWriteLock assignmentsIPRWLock, ReentrantReadWriteLock nonCachedMetricsLocalLock,
                                        AtomicLong assignmentsLastUpdatedLocal, ReentrantReadWriteLock balancerLock) {

        this.balancedMetricResolver = balancedMetricResolver;
        this.balancerConfig = balancerConfig;
        this.metricToHostMap = metricToHostMap;
        this.nonCachedMetrics = nonCachedMetrics;
        this.assignmentsIPRWLock = assignmentsIPRWLock;
        this.nonCachedMetricsLocalLock = nonCachedMetricsLocalLock;
        this.assignmentsLastUpdatedLocal = assignmentsLastUpdatedLocal;
        this.balancerLock = balancerLock;
        assignmentsLastUpdatedInHdfs = new DistributedAtomicLong(curatorFramework, ASSIGNMENTS_LAST_UPDATED_PATH,
                new RetryForever(1000));
    }

    public long getAssignmentsLastUpdatedByLeader() throws Exception {
        return assignmentsLastUpdatedInHdfs.get().postValue();
    }

    public void readAssignments(boolean checkIfNecessary) {

        try {
            long lastLocalUpdate = assignmentsLastUpdatedLocal.get();
            long lastHdfsUpdate = assignmentsLastUpdatedInHdfs.get().postValue();
            if (checkIfNecessary) {
                if (lastHdfsUpdate <= lastLocalUpdate) {
                    LOG.debug("Not reading assignments from storage lastStorageUpdate ({}) <= lastLocalUpdate ({})",
                            new Date(lastHdfsUpdate), new Date(lastLocalUpdate));
                    return;
                }
            }
            LOG.debug("Reading assignments from storage lastStorageUpdate ({}) > lastLocalUpdate ({})",
                    new Date(lastHdfsUpdate), new Date(lastLocalUpdate));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        // proceed with reading from storage

        try (InputStream inputStream = getInputStream(balancerConfig)) {
            boolean acquired = false;
            while (!acquired) {
                acquired = assignmentsIPRWLock.readLock().acquire(60, TimeUnit.SECONDS);
            }
            balancerLock.writeLock().lock();
            // get potential new list from implementation
            Map<String, TimelyBalancedHost> assignedMetricToHostMap = readFromStream(inputStream);
            metricToHostMap.clear();
            for (Map.Entry<String, TimelyBalancedHost> e : assignedMetricToHostMap.entrySet()) {
                if (StringUtils.isNotBlank(e.getKey()) && e.getValue() != null) {
                    if (balancedMetricResolver.shouldCache(e.getKey())) {
                        balancedMetricResolver.metricAssignedEvent(e.getKey(), null, e.getValue(), MetricAssignedEvent.Reason.ASSIGN_FILE);
                        metricToHostMap.put(e.getKey(), e.getValue());
                    }
                } else {
                    Exception e1 = new IllegalStateException(
                            "Bad assignment metric:" + e.getKey() + " host:" + e.getValue());
                    LOG.warn(e1.getMessage(), e1);
                }
            }
            nonCachedMetricsLocalLock.readLock().lock();
            try {
                Iterator<Map.Entry<String, TimelyBalancedHost>> itr = metricToHostMap.entrySet().iterator();
                while (itr.hasNext()) {
                    Map.Entry<String, TimelyBalancedHost> e = itr.next();
                    if (nonCachedMetrics.contains(e.getKey())) {
                        itr.remove();
                    }
                }
            } finally {
                nonCachedMetricsLocalLock.readLock().unlock();
            }
            assignmentsLastUpdatedLocal.set(assignmentsLastUpdatedInHdfs.get().postValue());
            LOG.info("Read {} assignments from storage lastStorageUpdate = lastLocalUpdate ({})", metricToHostMap.size(),
                    new Date(assignmentsLastUpdatedLocal.get()));
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            balancerLock.writeLock().unlock();
            try {
                assignmentsIPRWLock.readLock().release();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public void writeAssignments() {
        writeAssignments(metricToHostMap);
    }

    protected void writeAssignments(Map<String, TimelyBalancedHost> metricToHostMap) {

        try {
            boolean acquired = false;
            while (!acquired) {
                acquired = assignmentsIPRWLock.writeLock().acquire(60, TimeUnit.SECONDS);
            }
            balancerLock.readLock().lock();
            nonCachedMetricsLocalLock.readLock().lock();
            try (OutputStream outputStream = getOutputStream(balancerConfig)) {
                if (!metricToHostMap.isEmpty()) {
                    writeToStream(outputStream);
                    long now = System.currentTimeMillis();
                    assignmentsLastUpdatedLocal.set(now);
                    assignmentsLastUpdatedInHdfs.trySet(now);
                    if (!assignmentsLastUpdatedInHdfs.get().succeeded()) {
                        assignmentsLastUpdatedInHdfs.forceSet(now);
                    }
                    LOG.debug("Wrote {} assignments to storage lastStorageUpdate = lastLocalUpdate ({})",
                            metricToHostMap.size(), new Date(assignmentsLastUpdatedLocal.get()));
                }
            } finally {
                nonCachedMetricsLocalLock.readLock().unlock();
                balancerLock.readLock().unlock();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            try {
                assignmentsIPRWLock.writeLock().release();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public Map<String, TimelyBalancedHost> readFromStream(InputStream inputStream) throws IOException {
        Map<String, TimelyBalancedHost> assignedMetricToHostMap = new TreeMap<>();
        try {
            CsvReader reader = new CsvReader(inputStream, ',', Charset.forName("UTF-8"));
            reader.setUseTextQualifier(false);

            // skip the headers
            boolean success = true;
            success = reader.readHeaders();

            while (success) {
                success = reader.readRecord();
                String[] nextLine = reader.getValues();
                if (nextLine.length >= 3) {
                    String metric = nextLine[0];
                    String host = nextLine[1];
                    int tcpPort = Integer.parseInt(nextLine[2]);
                    TimelyBalancedHost tbh = balancedMetricResolver.findHost(host, tcpPort);
                    if (tbh == null) {
                        tbh = balancedMetricResolver.getRoundRobinHost(null);
                    } else {
                        LOG.trace("Found assigment: {} to {}:{}", metric, host, tcpPort);
                    }
                    assignedMetricToHostMap.put(metric, tbh);
                }
            }
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw e;
        }
        return assignedMetricToHostMap;
    }

    private void writeToStream(OutputStream outputStream) throws IOException {

        CsvWriter writer = null;
        try {
            writer = new CsvWriter(outputStream, ',', Charset.forName("UTF-8"));
            writer.setUseTextQualifier(false);
            writer.write("metric");
            writer.write("host");
            writer.write("tcpPort");
            writer.endRecord();
            for (Map.Entry<String, TimelyBalancedHost> e : metricToHostMap.entrySet()) {
                if (!nonCachedMetrics.contains(e.getKey())) {
                    writer.write(e.getKey());
                    writer.write(e.getValue().getHost());
                    writer.write(Integer.toString(e.getValue().getTcpPort()));
                    writer.endRecord();
                    LOG.trace("Saving assigment: {} to {}:{}", e.getKey(), e.getValue().getHost(),
                            e.getValue().getTcpPort());
                }
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }

    public InputStream getInputStream(BalancerConfiguration balancerConfiguration) throws Exception {
        PeristerType peristerType = PeristerType.valueOf(balancerConfiguration.getMetricAssignmentPersisterType());
        switch (peristerType) {
            case HDFS:
                Configuration configuration = new Configuration();
                FileSystem fs = FileSystem.get(configuration);
                Path assignmentFile = new Path(balancerConfiguration.getAssignmentFile());
                return fs.open(assignmentFile);
            case FILE:
                return new FileInputStream(new File(balancerConfiguration.getAssignmentFile()));
            default:
                throw new IllegalArgumentException("Unknown metricPersisterType " + balancerConfiguration.getMetricAssignmentPersisterType());
        }
    }

    public OutputStream getOutputStream(BalancerConfiguration balancerConfiguration) throws IOException {
        PeristerType peristerType = PeristerType.valueOf(balancerConfiguration.getMetricAssignmentPersisterType());
        switch (peristerType) {
            case HDFS:
                Configuration configuration = new Configuration();
                FileSystem fs = FileSystem.get(configuration);
                Path assignmentFile = new Path(balancerConfiguration.getAssignmentFile());
                if (!fs.exists(assignmentFile.getParent())) {
                    fs.mkdirs(assignmentFile.getParent());
                }
                return fs.create(assignmentFile, true);
            case FILE:
                return new FileOutputStream(new File(balancerConfiguration.getAssignmentFile()));
            default:
                throw new IllegalArgumentException("Unknown metricPersisterType " + balancerConfiguration.getMetricAssignmentPersisterType());
        }
    }
}

package timely.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import timely.adapter.accumulo.MetricAdapter;
import timely.model.Metric;

public class TestMetricWriter {

    private static final int DEFAULT_BATCHES = 10000;
    private final AccumuloClient accumuloClient;
    private final TestMetrics metrics;
    private final String tableName;

    public TestMetricWriter(String tableName, AccumuloClient accumuloClient, TestMetrics metrics) {
        this.accumuloClient = accumuloClient;
        this.metrics = metrics;
        this.tableName = tableName;
    }

    public long ingestRandomDuration(long duration, TimeUnit durationUnit, long timestampInitial, int decrementMillis) {
        long timestampCurrent = timestampInitial;
        long runUntilMillis = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(duration, durationUnit);
        while (System.currentTimeMillis() < runUntilMillis) {
            timestampCurrent = ingestRandom(timestampCurrent, decrementMillis);
        }
        return timestampCurrent;
    }

    private long ingestRandom(long timestampInitial, int decrementMillis) {
        long ts = timestampInitial;
        Collection<Mutation> mutations = new ArrayList<>(TestMetricWriter.DEFAULT_BATCHES);
        int idx = 0;
        while (idx++ < TestMetricWriter.DEFAULT_BATCHES) {
            ts = ts - decrementMillis;
            Metric m = metrics.randomMetric(ts);
            Mutation mutation = MetricAdapter.toMutation(m);
            mutations.add(mutation);
        }

        try {
            BatchWriterConfig cfg = new BatchWriterConfig().setMaxWriteThreads(4);
            try (BatchWriter writer = accumuloClient.createBatchWriter(tableName, cfg)) {
                writer.addMutations(mutations);
            }
            accumuloClient.tableOperations().flush(tableName, null, null, true);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            throw new RuntimeException(e);
        }

        return ts;
    }
}

package timely.server.store;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Assert;
import org.junit.Test;

import timely.accumulo.MetricAdapter;
import timely.model.Metric;
import timely.server.test.TestMetrics;

public class MutationsTest {

    private final TestMetrics metrics = new TestMetrics();

    @Test
    public void testMutations() {
        long iterations = 65536;
        long delta = Long.MAX_VALUE / iterations * 2;
        long ts = Long.MIN_VALUE;

        for (int i = 0; i < iterations; i++) {
            Metric m = metrics.randomMetric(ts);
            Mutation mutation = MetricAdapter.toMutation(m);
            for (ColumnUpdate update : mutation.getUpdates()) {
                Key key = new Key(mutation.getRow(), update.getColumnFamily(), update.getColumnQualifier(), update.getColumnVisibility(),
                                update.getTimestamp());
                long time = MetricAdapter.decodeRowKey(key).getSecond();
                Assert.assertEquals("timestamps " + update.getTimestamp() + " expected to be " + ts, ts, update.getTimestamp());
                Assert.assertEquals("time " + time + " expected to be " + MetricAdapter.roundTimestampToLastHour(ts),
                                MetricAdapter.roundTimestampToLastHour(ts), time);
                long diff = Math.abs(time - ts);
                Assert.assertTrue("encoded time " + time + " not close to " + ts, diff < 3600000);
            }
            ts += delta;
        }
    }
}

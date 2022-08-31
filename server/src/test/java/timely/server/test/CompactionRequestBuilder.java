package timely.server.test;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.dataImpl.TabletIdImpl;
import org.apache.accumulo.core.metadata.StoredTabletFile;
import org.apache.accumulo.core.metadata.schema.DataFileValue;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.server.ServerContext;
import org.apache.accumulo.tserver.compaction.MajorCompactionReason;
import org.apache.accumulo.tserver.compaction.MajorCompactionRequest;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;

import timely.accumulo.MetricAdapter;

public class CompactionRequestBuilder {

    private static final long DEFAULT_TIME_MILLIS = new GregorianCalendar(2019, Calendar.JANUARY, 1).getTimeInMillis();
    private static final Lexicoder<String> stringEncoder = new StringLexicoder();
    private static final PairLexicoder<String,String> badPairEncoder = new PairLexicoder<>(stringEncoder, stringEncoder);

    private final Map<StoredTabletFile,DataFileValue> refs = new LinkedHashMap<>();
    private final Map<String,String> tableProperties;
    private final long timeMillis;
    private Key endKeyMetric;
    private Key prevKeyMetric;

    public CompactionRequestBuilder() {
        this(DEFAULT_TIME_MILLIS);
    }

    public CompactionRequestBuilder(long timeMillis) {
        this.timeMillis = timeMillis;
        this.tableProperties = new TreeMap<>();
    }

    public CompactionRequestBuilder endKeyMetricNonOffset(String name, String offset) {
        byte[] rowKey = offset != null ? badPairEncoder.encode(new ComparablePair<>(name, offset)) : stringEncoder.encode(name);
        endKeyMetric = new Key(rowKey);
        return this;
    }

    public CompactionRequestBuilder endKeyMetric(String name, long offset) {
        long ts = timeMillis - offset;
        byte[] rowKey = MetricAdapter.encodeRowKey(name, ts);
        endKeyMetric = new Key(rowKey);
        return this;
    }

    public CompactionRequestBuilder prevEndKeyMetric(String name, long offset) {
        long ts = timeMillis - offset;
        byte[] rowKey = MetricAdapter.encodeRowKey(name, ts);
        prevKeyMetric = new Key(rowKey);
        return this;
    }

    public CompactionRequestBuilder prevEndKeyMetricNonOffset(String name, String offset) {
        byte[] rowKey = offset != null ? badPairEncoder.encode(new ComparablePair<>(name, offset)) : stringEncoder.encode(name);
        prevKeyMetric = new Key(rowKey);
        return this;
    }

    public CompactionRequestBuilder file(String path, long size, long entries) {
        refs.put(new StoredTabletFile(path), new DataFileValue(size, entries));
        return this;
    }

    public CompactionRequestBuilder tableProperties(String key, String value) {
        tableProperties.put(key, value);
        return this;
    }

    public MajorCompactionRequest build() {
        TableId tableId = TableId.of("1");
        Text endRow = null;
        if (null != endKeyMetric) {
            endRow = endKeyMetric.getRow();
        }

        Text prevEndRow = null;
        if (null != prevKeyMetric) {
            prevEndRow = prevKeyMetric.getRow();
        }
        KeyExtent ke = new KeyExtent(tableId, endRow, prevEndRow);
        TabletId tid = new TabletIdImpl(ke);

        // @formatter:off
        MajorCompactionRequest req = EasyMock.partialMockBuilder(MajorCompactionRequest.class)
                .withConstructor(KeyExtent.class, MajorCompactionReason.class,
                        AccumuloConfiguration.class, ServerContext.class)
                .withArgs(ke, MajorCompactionReason.NORMAL, DefaultConfiguration.getInstance(), null)
                .addMockedMethod("getTabletId")
                .addMockedMethod("getTableProperties")
                .createMock();

        // @formatter:on
        req.setFiles(refs);
        EasyMock.expect(req.getTabletId()).andStubReturn(tid);
        EasyMock.expect(req.getTableProperties()).andStubReturn(tableProperties);

        EasyMock.replay(req);

        return req;
    }
}

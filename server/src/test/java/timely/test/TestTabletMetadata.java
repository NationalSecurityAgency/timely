package timely.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.util.ColumnFQ;
import org.apache.hadoop.io.Text;

public class TestTabletMetadata {

    private final String tableId;
    private final List<Map.Entry<Key, Value>> entries;

    public TestTabletMetadata(String tableId) {
        this.tableId = tableId;
        entries = new ArrayList<>();
    }

    public TestTabletMetadata file(Text row, String path, String value) {
        Text cq = new Text(path);
        addEntry(row, MetadataSchema.TabletsSection.DataFileColumnFamily.NAME, cq, value);
        return this;
    }

    public TestTabletMetadata prev(Text row, Text prevRow) {
        ColumnFQ cfq = MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN;
        addEntry(row, cfq.getColumnFamily(), cfq.getColumnQualifier(), KeyExtent.encodePrevEndRow(prevRow));
        return this;
    }

    public TestTabletMetadata time(Text row, long millis) {
        ColumnFQ cfq = MetadataSchema.TabletsSection.ServerColumnFamily.TIME_COLUMN;
        addEntry(row, cfq.getColumnFamily(), cfq.getColumnQualifier(), "M" + millis);
        return this;
    }

    public Collection<Map.Entry<Key, Value>> entries() {
        return entries;
    }

    private void addEntry(Text row, Text cf, Text cq, String value) {
        addEntry(row, cf, cq, new Value(value));
    }

    private void addEntry(Text row, Text cf, Text cq, Value value) {
        Text rowEntry = KeyExtent.getMetadataEntry(tableId, row);
        Value val = new Value(value);
        Key k = new Key(rowEntry, cf, cq);
        entries.add(Maps.immutableEntry(k, val));
    }
}

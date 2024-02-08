package timely.store.compaction.util;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.FormatterConfig;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import timely.adapter.accumulo.MetricAdapter;
import timely.test.TestTabletMetadata;

public class TabletMetadataFormatterTest {

    @Test
    public void simpleTextOutput() {
        Long a1 = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(30);
        Long a2 = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(29);
        Text r1 = new Text(MetricAdapter.encodeRowKey("sys.cpu", a1));
        Text r2 = new Text(MetricAdapter.encodeRowKey("sys.cpu", a2));

        // @formatter:off
        Collection<Map.Entry<Key, Value>> metadata = new TestTabletMetadata("2")
                .file(r1, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", "800,3")
                .file(r1, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao9.rf", "431,1")
                .prev(r1, null)
                .time(r1, a1)
                .file(r2, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", "800,3")
                .file(r2, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao9.rf", "431,1")
                .file(r2, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao0.rf", "451,2")
                .time(r2, a2)
                .prev(r2, r1)
                .entries();

        // @formatter:on
        TabletMetadataFormatter f = new TabletMetadataFormatter();
        f.initialize(metadata, new FormatterConfig());
        System.out.println(f.next());
    }
}

package timely.store.compaction.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.collect.Lists;

import timely.adapter.accumulo.MetricAdapter;
import timely.test.TestTabletMetadata;

public class TabletMetadataViewTest {

    @Test
    public void tabletAccumulate() {
        Text r1 = new Text(MetricAdapter.encodeRowKey("sys.cpu", 1L));
        Text r2 = new Text(MetricAdapter.encodeRowKey("sys.mem", 1L));
        Text r3 = new Text(MetricAdapter.encodeRowKey("sys.disk", 1L));

        // @formatter:off
        Collection<Map.Entry<Key, Value>> metadata = new TestTabletMetadata("2")
                .file(r1, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", "800,3")
                .file(r1, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao9.rf", "431,1")
                .prev(r1, null)
                .time(r1, 1)
                .time(r2, 2)
                .prev(r2, r3)
                .file(r3, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao8.rf", "800,3")
                .file(r3, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao9.rf", "431,1")
                .file(r3, "hdfs://hdfs-name/accumulo/tables/2/default_tablet/C0000ao0.rf", "451,2")
                .time(r3, 3)
                .prev(r3, r1)
                .entries();

        // assert that r1/r2 are in the output correctly
        // r2 does not have any files, it should not appear in the output
        // @formatter:on
        TabletMetadataView view = new TabletMetadataView();
        view.addEntry(metadata);
        List<MetadataAccumulator.Entry> accumulated = Lists.newArrayList(view.getEntries());
        assertEquals(2, accumulated.size());

        // validate that it skipped over non-file
        assertEquals(r1, accumulated.get(0).getTablet());
        assertEquals("sys.cpu", accumulated.get(0).getTabletPrefix());
        assertEquals(1, accumulated.get(0).getMilliseconds());
        assertEquals(2, accumulated.get(0).getTotalFiles());
        assertEquals(r3, accumulated.get(1).getTablet());
        assertEquals("sys.disk", accumulated.get(1).getTabletPrefix());
        assertEquals(3, accumulated.get(1).getTotalFiles());
        assertEquals(3, accumulated.get(1).getMilliseconds());

        assertNull(accumulated.get(0).getTabletPrev());
        assertEquals(r1, accumulated.get(1).getTabletPrev());
    }

    @Test
    public void noRowsOnView() {
        Collection<Map.Entry<Key,Value>> metadata = Collections.emptyList();
        TabletMetadataView view = new TabletMetadataView();
        view.addEntry(metadata);
        List<MetadataAccumulator.Entry> accumulated = Lists.newArrayList(view.getEntries());
        TabletSummary summary = view.computeSummary().build();
        String text = view.toText();

        assertEquals(0, accumulated.size());
        assertEquals(summary.totalTablets(), 0);
        assertNotNull(text);
    }

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
        TabletMetadataView view = new TabletMetadataView();
        view.addEntry(metadata);
        String output = view.toText(TimeUnit.DAYS);
        System.out.println(output);
    }
}

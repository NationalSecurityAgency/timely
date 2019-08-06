package timely.store.compaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.OptionalLong;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.hadoop.io.Text;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class TabletRowAdapterTest {

    private static final Lexicoder<String> prefixCoder = new StringLexicoder();
    private static final Lexicoder<Long> offsetCoder = new LongLexicoder();
    private static final PairLexicoder<String, Long> pairCoder = new PairLexicoder<>(prefixCoder, offsetCoder);

    @Test
    public void decodePrefix() {
        ComparablePair<String, Long> p = new ComparablePair<>("test", 1L);
        byte[] bytes = pairCoder.encode(p);
        OptionalLong decoded = TabletRowAdapter.decodeRowOffset(new Text(bytes));
        assertTrue(decoded.isPresent());
    }

    @Test
    public void decodeOffsetOnly() {
        ComparablePair<String, Long> p = new ComparablePair<>("test", 1L);
        byte[] bytes = pairCoder.encode(p);
        OptionalLong decoded = TabletRowAdapter.decodeRowOffset(new Text(bytes));
        assertTrue(decoded.isPresent());
        assertEquals(1L, decoded.getAsLong());
    }

    @Test
    public void decodeOffsetWithTruncatedOffset() {
        // offset is truncated on tablet extents
        long truncateOffset = 1554962382848L;
        ComparablePair<String, Long> p = new ComparablePair<>("sys.disk.disk_octets", truncateOffset);
        byte[] bytes = pairCoder.encode(p);
        bytes = Arrays.copyOf(bytes, bytes.length - 4);
        OptionalLong decoded = TabletRowAdapter.decodeRowOffset(new Text(bytes));
        assertTrue(decoded.isPresent());
        assertEquals(truncateOffset, decoded.getAsLong());
    }

    @Test
    public void decodeOffsetWithBadDataWillSet() {
        ComparablePair<String, String> p = new ComparablePair<>("test", "<");
        byte[] bytes = new PairLexicoder<>(prefixCoder, prefixCoder).encode(p);
        OptionalLong decoded = TabletRowAdapter.decodeRowOffset(new Text(bytes));
        assertFalse(decoded.isPresent());
    }

    @Test
    public void debugOutputTest() {
        long offset = 1554962382848L;
        ComparablePair<String, Long> p = new ComparablePair<>("sys.disk.disk_octets", offset);
        byte[] bytes = pairCoder.encode(p);
        TabletId tid = EasyMock.createMock(TabletId.class);
        EasyMock.expect(tid.getEndRow()).andReturn(new Text(bytes));
        EasyMock.replay(tid);

        String debug = TabletRowAdapter.toDebugOutput(tid);
        assertThat(debug, CoreMatchers.containsString("prefix: sys.disk.disk_octets"));
        assertThat(debug, CoreMatchers.containsString("date: 2019-04-11T05:59:42.848"));
        assertThat(debug, CoreMatchers.containsString("millis: 1554962382848"));
    }
}

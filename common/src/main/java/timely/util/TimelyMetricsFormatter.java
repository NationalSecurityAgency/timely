package timely.util;

import java.nio.ByteBuffer;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.PairLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.accumulo.core.util.format.DefaultFormatter;

public class TimelyMetricsFormatter extends DefaultFormatter {

    private static final PairLexicoder<String,Long> rowCoder = new PairLexicoder<>(new StringLexicoder(), new LongLexicoder());
    private static final PairLexicoder<Long,String> colQualCoder = new PairLexicoder<>(new LongLexicoder(), new StringLexicoder());

    @Override
    public String next() {
        Entry<Key,Value> entry = getScannerIterator().next();
        return formatMetricEntry(entry, super.isDoTimestamps());
    }

    public static String formatMetricEntry(Entry<Key,Value> entry, boolean showTimestamps) {
        StringBuilder b = new StringBuilder();
        ComparablePair<String,Long> p = rowCoder.decode(entry.getKey().getRow().copyBytes());
        b.append(p.getFirst()).append("\\x00").append(p.getSecond());
        b.append(" ");
        b.append(entry.getKey().getColumnFamily().toString());
        b.append(":");
        ComparablePair<Long,String> cq = colQualCoder.decode(entry.getKey().getColumnQualifier().copyBytes());
        b.append(cq.getFirst()).append("\\x00").append(cq.getSecond());
        b.append(" ");
        b.append(entry.getKey().getColumnVisibility().toString());
        if (showTimestamps) {
            b.append(" ");
            b.append(entry.getKey().getTimestamp());
        }
        b.append(" ");
        ByteBuffer.wrap(entry.getValue().get());
        b.append(ByteBuffer.wrap(entry.getValue().get()).getDouble());
        return b.toString();
    }
}

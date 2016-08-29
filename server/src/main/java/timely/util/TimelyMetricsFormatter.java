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

    private static final PairLexicoder<String, Long> rowCoder = new PairLexicoder<>(new StringLexicoder(),
            new LongLexicoder());

    @Override
    public String next() {
        Entry<Key, Value> e = getScannerIterator().next();
        StringBuilder b = new StringBuilder();
        ComparablePair<String, Long> p = rowCoder.decode(e.getKey().getRow().copyBytes());
        b.append(p.getFirst()).append("\\x00").append(p.getSecond());
        b.append(" ");
        b.append(e.getKey().getColumnFamily().toString());
        b.append(":");
        b.append(e.getKey().getColumnQualifier().toString());
        b.append(" ");
        b.append(e.getKey().getColumnVisibility().toString());
        if (super.isDoTimestamps()) {
            b.append(" ");
            b.append(e.getKey().getTimestamp());
        }
        b.append(" ");
        ByteBuffer.wrap(e.getValue().get());
        b.append(ByteBuffer.wrap(e.getValue().get()).getDouble());
        return b.toString();
    }

}

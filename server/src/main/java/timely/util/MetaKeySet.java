package timely.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

public class MetaKeySet extends TreeSet<Key> {

    public static class MetaKeyComparator implements Comparator<Key>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Key k1, Key k2) {
            int row = k1.getRow().compareTo(k2.getRow());
            if (row == 0) {
                int colf = k1.getColumnFamily().compareTo(k2.getColumnFamily());
                if (colf == 0) {
                    return k1.getColumnQualifier().compareTo(k2.getColumnQualifier());
                } else {
                    return colf;
                }
            } else {
                return row;
            }
        }

    }

    public static final Value NULL_VALUE = new Value(new byte[0]);
    private static final long serialVersionUID = 1L;

    public MetaKeySet() {
        super(new MetaKeyComparator());
    }

    public List<Mutation> toMutations(long ts) {
        List<Mutation> results = new ArrayList<>();
        String prevRow = null;
        Iterator<Key> keys = this.iterator();
        Mutation m = null;
        while (keys.hasNext()) {
            Key next = keys.next();
            if (null == prevRow || !(prevRow.equals(next.getRow().toString()))) {
                if (null != prevRow) {
                    results.add(m);
                }
                m = new Mutation(next.getRow());
                prevRow = next.getRow().toString();
            }
            m.put(next.getColumnFamily(), next.getColumnQualifier(), ts, NULL_VALUE);
        }
        results.add(m);
        return results;
    }

}

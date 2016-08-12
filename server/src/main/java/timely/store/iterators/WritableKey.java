package timely.store.iterators;

import org.apache.accumulo.core.data.Key;

public class WritableKey extends Key {

    public WritableKey(Key k) {
        super(k);
    }

    public void setRow(byte[] r) {
        this.row = r;
    }
}

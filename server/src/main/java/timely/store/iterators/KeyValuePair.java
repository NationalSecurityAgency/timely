package timely.store.iterators;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class KeyValuePair {

    protected Key key;
    protected Value value;

    public Key getKey() {
        return key;
    }

    public Value getValue() {
        return value;
    }

    public void setKey(Key f) {
        this.key = f;
    }

    public void setValue(Value s) {
        this.value = s;
    }

    public void empty() {
        this.key = null;
        this.value = null;
    }

    public boolean isEmpty() {
        return (this.key == null && this.value == null);
    }

}

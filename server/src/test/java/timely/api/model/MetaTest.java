package timely.api.model;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.Assert;
import org.junit.Test;
import timely.adapter.accumulo.MetaAdapter;

public class MetaTest {

    @Test
    public void testToKeys() {
        Meta one = new Meta("sys.cpu.user", "tag1", "value1");
        List<Key> keys = MetaAdapter.toKeys(one);
        Assert.assertTrue(keys.contains(new Key("m:sys.cpu.user")));
        Assert.assertTrue(keys.contains(new Key("t:sys.cpu.user", "tag1")));
        Assert.assertTrue(keys.contains(new Key("v:sys.cpu.user", "tag1", "value1")));
    }

}

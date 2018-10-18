package timely.util;

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.junit.Assert;
import org.junit.Test;
import timely.api.model.Meta;

public class MetaKeySetTest {

    @Test
    public void testContents() {
        Meta one = new Meta("sys.cpu.user", "tag1", "value1");
        Meta two = new Meta("sys.cpu.user", "tag2", "value2");
        Meta three = new Meta("sys.cpu.user", "tag3", "value3");
        MetaKeySet mks = new MetaKeySet();
        mks.addAll(one.toKeys());
        mks.addAll(two.toKeys());
        mks.addAll(three.toKeys());
        Assert.assertEquals(7, mks.size());
        Assert.assertTrue(mks.contains(new Key("m:sys.cpu.user")));
        Assert.assertTrue(mks.contains(new Key("t:sys.cpu.user", "tag1")));
        Assert.assertTrue(mks.contains(new Key("t:sys.cpu.user", "tag2")));
        Assert.assertTrue(mks.contains(new Key("t:sys.cpu.user", "tag3")));
        Assert.assertTrue(mks.contains(new Key("v:sys.cpu.user", "tag1", "value1")));
        Assert.assertTrue(mks.contains(new Key("v:sys.cpu.user", "tag2", "value2")));
        Assert.assertTrue(mks.contains(new Key("v:sys.cpu.user", "tag3", "value3")));
    }

    @Test
    public void testToMutations() {
        Meta one = new Meta("sys.cpu.user", "tag1", "value1");
        Meta two = new Meta("sys.cpu.user", "tag2", "value2");
        Meta three = new Meta("sys.cpu.user", "tag3", "value3");
        MetaKeySet mks = new MetaKeySet();
        mks.addAll(one.toKeys());
        mks.addAll(two.toKeys());
        mks.addAll(three.toKeys());
        List<Mutation> muts = mks.toMutations();
        Mutation e1 = new Mutation("m:sys.cpu.user");
        e1.put("", "", MetaKeySet.NULL_VALUE);
        Mutation e2 = new Mutation("t:sys.cpu.user");
        e2.put("tag1", "", MetaKeySet.NULL_VALUE);
        e2.put("tag2", "", MetaKeySet.NULL_VALUE);
        e2.put("tag3", "", MetaKeySet.NULL_VALUE);
        Mutation e3 = new Mutation("v:sys.cpu.user");
        e3.put("tag1", "value1", MetaKeySet.NULL_VALUE);
        e3.put("tag2", "value2", MetaKeySet.NULL_VALUE);
        e3.put("tag3", "value3", MetaKeySet.NULL_VALUE);
        Assert.assertEquals(3, muts.size());
        Assert.assertTrue(muts.contains(e1));
        Assert.assertTrue(muts.contains(e2));
        Assert.assertTrue(muts.contains(e3));
    }

}

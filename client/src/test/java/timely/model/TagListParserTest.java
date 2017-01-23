package timely.model;

import org.junit.Assert;
import org.junit.Test;
import timely.model.parse.TagListParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class TagListParserTest {

    @Test
    public void testListParse() {
        String value = "tag1=value1,tag2=value2";
        List<Tag> tags = new TagListParser().parse(value);
        Assert.assertEquals(2, tags.size());
        Assert.assertEquals(new Tag("tag1", "value1"), tags.get(0));
        Assert.assertEquals(new Tag("tag2", "value2"), tags.get(1));
    }

    @Test
    public void testListParseEncoded() {
        String value = "tag%2C%3D%31=value%2C%3D%31,tag%2C%3D%32=value%2C%3D%32";
        List<Tag> tags = new TagListParser().parse(value);
        Assert.assertEquals(2, tags.size());
        Assert.assertEquals(new Tag("tag,=1", "value,=1"), tags.get(0));
        Assert.assertEquals(new Tag("tag,=2", "value,=2"), tags.get(1));
    }

    @Test
    public void testListCombine() {
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        tags.add(new Tag("tag2", "value2"));
        String combined = new TagListParser().combine(tags);
        Assert.assertEquals("tag1=value1,tag2=value2", combined);
    }

    @Test
    public void testListCombineEncoded() {
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag,=1", "value,=1"));
        tags.add(new Tag("tag,=2", "value,=2"));
        String combined = new TagListParser().combine(tags);
        Assert.assertEquals("tag%2C%3D1=value%2C%3D1,tag%2C%3D2=value%2C%3D2", combined);
    }

    @Test
    public void testListCombineMap() {
        Map<String, String> map = new TreeMap<>();
        map.put("tag1", "value1");
        map.put("tag2", "value2");
        String combined = new TagListParser().combine(map);
        Assert.assertEquals("tag1=value1,tag2=value2", combined);
    }
}

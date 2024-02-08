package timely.model.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import timely.model.Tag;

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
    public void testListCombine() {
        List<Tag> tags = new ArrayList<>();
        tags.add(new Tag("tag1", "value1"));
        tags.add(new Tag("tag2", "value2"));
        String combined = new TagListParser().combine(tags);
        Assert.assertEquals("tag1=value1,tag2=value2", combined);
    }

    @Test
    public void testListCombineMap() {
        Map<String,String> map = new TreeMap<>();
        map.put("tag1", "value1");
        map.put("tag2", "value2");
        String combined = new TagListParser().combine(map);
        Assert.assertEquals("tag1=value1,tag2=value2", combined);
    }

    @Test
    public void testParseTagsWithCommas() {

        try {
            String s = "tag1=value1,tag2=3.4.3_(default\\,_Date\\,_Time)_";
            new TagListParser().parse(s);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }
}

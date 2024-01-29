package timely.model.parse;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import timely.model.Metric;
import timely.model.Tag;

public class MetricParserTest {

    @Test
    public void testParseWithEscapedCharacters() {

        MetricParser parser = new MetricParser();
        Metric m = parser.parse("put mymetric 12341234 5.0 tag1=value1,value1 tag2=value2=value2");

        Assert.assertEquals("mymetric", m.getName());
        Assert.assertEquals(12341234, (long) m.getValue().getTimestamp());
        Assert.assertEquals(5.0, (double) m.getValue().getMeasure(), 0);
        List<Tag> expected = new ArrayList<>();
        expected.add(new Tag("tag1", "value1,value1"));
        expected.add(new Tag("tag2", "value2=value2"));
        Assert.assertEquals(expected, m.getTags());
    }

    @Test
    public void testParseMalformatted() {

        MetricParser parser = new MetricParser();
        try {
            // parser should throw an exception
            parser.parse("put mymetric 12341234 5.0 tag1 tag2=value2");
            Assert.fail();
        } catch (IllegalArgumentException e) {

        }
    }
}

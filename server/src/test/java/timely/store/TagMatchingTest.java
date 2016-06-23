package timely.store;

import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.junit.Test;

public class TagMatchingTest {

    @Test
    public void testRegex1() throws Exception {
        String tags = "tag1=value1,tag2=value2,tag3=value3";
        StringBuffer pattern = new StringBuffer();
        pattern.append("(^|.*,)");
        pattern.append("tag2");
        pattern.append("=");
        pattern.append("value2");
        pattern.append("(,.*|$)");
        Pattern p = Pattern.compile(pattern.toString());
        assertTrue(p.matcher(tags).matches());
    }

    @Test
    public void testRegex2() throws Exception {
        String tags = "tag1=value1,tag2=value2,tag3=value3";
        StringBuffer pattern = new StringBuffer();
        pattern.append("(^|.*,)");
        pattern.append("tag2");
        pattern.append("=");
        pattern.append("value\\d");
        pattern.append("(,.*|$)");
        Pattern p = Pattern.compile(pattern.toString());
        assertTrue(p.matcher(tags).matches());
    }

    @Test
    public void testRegex3() throws Exception {
        String tags = "tag1=value1,tag2=value2,tag3=value3";
        StringBuffer pattern = new StringBuffer();
        pattern.append("(^|.*,)");
        pattern.append("tag2");
        pattern.append("=");
        pattern.append("(value2|value3)");
        pattern.append("(,.*|$)");
        Pattern p = Pattern.compile(pattern.toString());
        assertTrue(p.matcher(tags).matches());
    }

}

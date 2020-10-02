package timely.api.response.timeseries;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;
import timely.util.JsonUtil;

public class QueryResponseTest {

    @Test
    public void testEmptyResponse() throws Exception {
        String r = JsonUtil.getObjectMapper().writeValueAsString(Collections.emptyList());
        Assert.assertEquals("[]", r);
    }

    @Test
    public void testOneResponse() throws Exception {
        QueryResponse r = new QueryResponse();
        r.setMetric("sys.cpu.user");
        r.putTag("host", "localhost");
        r.putTag("rack", "r1");
        r.putDps("1234567890", 4.5);
        r.putDps("1234567900", 3.5);
        r.putDps("1234567910", 2.5);
        String result = JsonUtil.getObjectMapper().writeValueAsString(Collections.singletonList(r));
        System.out.println("result: " + result);
        String expectedMetric = "\"metric\":\"sys.cpu.user\"";
        Assert.assertTrue(result.contains(expectedMetric));
        String expectedTagsV1 = "\"tags\":{\"rack\":\"r1\",\"host\":\"localhost\"}";
        String expectedTagsV2 = "\"tags\":{\"host\":\"localhost\",\"rack\":\"r1\"}";
        Assert.assertTrue(result.contains(expectedTagsV1) || result.contains(expectedTagsV2));
        String expectedAggregatedTags = "\"aggregatedTags\":[]";
        Assert.assertTrue(result.contains(expectedAggregatedTags));
        String expectedDpsV1 = "\"dps\":{\"1234567890\":4.5,\"1234567900\":3.5,\"1234567910\":2.5}";
        String expectedDpsV2 = "\"dps\":{\"1234567890\":4.5,\"1234567910\":2.5,\"1234567900\":3.5}";
        String expectedDpsV3 = "\"dps\":{\"1234567900\":3.5,\"1234567890\":4.5,\"1234567910\":2.5}";
        String expectedDpsV4 = "\"dps\":{\"1234567900\":3.5,\"1234567910\":2.5,\"1234567890\":4.5}";
        String expectedDpsV5 = "\"dps\":{\"1234567910\":2.5,\"1234567890\":4.5,\"1234567900\":3.5}";
        String expectedDpsV6 = "\"dps\":{\"1234567910\":2.5,\"1234567900\":3.5,\"1234567890\":4.5,}";
        Assert.assertTrue(result.contains(expectedDpsV1) || result.contains(expectedDpsV2)
                || result.contains(expectedDpsV3) || result.contains(expectedDpsV4) || result.contains(expectedDpsV5)
                || result.contains(expectedDpsV6));
        Assert.assertEquals("[{", result.substring(0, 2));
        Assert.assertEquals("}]", result.substring(result.length() - 2));
    }
}

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
        String expected = "[{\"metric\":\"sys.cpu.user\",\"tags\":{\"host\":\"localhost\",\"rack\":\"r1\"},\"aggregatedTags\":[],\"dps\":{\"1234567890\":4.5,\"1234567900\":3.5,\"1234567910\":2.5}}]";
        Assert.assertEquals(expected, result);
    }
}

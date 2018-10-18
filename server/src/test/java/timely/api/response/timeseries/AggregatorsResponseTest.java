package timely.api.response.timeseries;

import org.junit.Assert;
import org.junit.Test;
import timely.util.JsonUtil;

public class AggregatorsResponseTest {

    @Test
    public void testAggregatorsResponseEmpty() throws Exception {
        AggregatorsResponse response = new AggregatorsResponse();
        String r = JsonUtil.getObjectMapper().writeValueAsString(response);
        Assert.assertEquals("[]", r);
    }

    @Test
    public void testAggregatorsResponse() throws Exception {
        AggregatorsResponse response = new AggregatorsResponse();
        response.addAggregator("min");
        response.addAggregator("max");
        String r = JsonUtil.getObjectMapper().writeValueAsString(response);
        Assert.assertEquals("[\"min\",\"max\"]", r);
    }

}

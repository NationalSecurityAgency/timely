package timely.api.query.response;

import org.junit.Assert;
import org.junit.Test;

import timely.util.JsonUtil;

public class SuggestResponseTest {

    @Test
    public void testSuggestResponseEmpty() throws Exception {
        SuggestResponse response = new SuggestResponse();
        String r = JsonUtil.getObjectMapper().writeValueAsString(response);
        Assert.assertEquals("[]", r);
    }

    @Test
    public void testSuggestResponse() throws Exception {
        SuggestResponse response = new SuggestResponse();
        response.addSuggestion("sys.cpu.idle");
        response.addSuggestion("sys.cpu.user");
        String r = JsonUtil.getObjectMapper().writeValueAsString(response);
        Assert.assertEquals("[\"sys.cpu.idle\",\"sys.cpu.user\"]", r);
    }

}

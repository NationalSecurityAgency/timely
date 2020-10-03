package timely.api.response.timeseries;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.response.timeseries.SearchLookupResponse.Result;
import timely.util.JsonUtil;

public class SearchLookupResponseTest {

    @Test
    public void testResponse1() throws Exception {
        SearchLookupResponse response = new SearchLookupResponse();
        response.setType("LOOKUP");
        response.setMetric("sys.cpu.user");
        response.putTag("host", "localhost");
        response.putTag("rack", "r1");
        response.setTime(1500);
        List<Result> results = new ArrayList<>();
        Result r1 = new Result();
        r1.setMetric("sys.cpu.idle");
        r1.setTsuid("000011000008203D00");
        r1.putTag("host", "localhost");
        r1.putTag("rack", "r1");
        Result r2 = new Result();
        r2.setMetric("sys.cpu.user");
        r2.setTsuid("000011000008203D01");
        r2.putTag("host", "localhost");
        r2.putTag("rack", "r1");
        results.add(r1);
        results.add(r2);
        response.setResults(results);
        response.setTotalResults(results.size());
        String r = JsonUtil.getObjectMapper().writeValueAsString(response);
        String expected = "{\"type\":\"LOOKUP\",\"metric\":\"sys.cpu.user\",\"tags\":{\"host\":\"localhost\",\"rack\":\"r1\"},\"limit\":0,\"time\":1500,\"totalResults\":2,\"results\":[{\"tags\":{\"host\":\"localhost\",\"rack\":\"r1\"},\"metric\":\"sys.cpu.idle\",\"tsuid\":\"000011000008203D00\"},{\"tags\":{\"host\":\"localhost\",\"rack\":\"r1\"},\"metric\":\"sys.cpu.user\",\"tsuid\":\"000011000008203D01\"}]}";
        Assert.assertEquals(expected, r);
        SearchLookupResponse slr = JsonUtil.getObjectMapper().readValue(r, SearchLookupResponse.class);
        Assert.assertEquals(response, slr);
    }

    @Test
    public void testMetricRequest() {
        try {
            String r = "{\"operation\":\"lookup\", \"metric\":\"testmetric\", \"tags\":[{\"host\":\".*\"}]}";
            SearchLookupRequest req = JsonUtil.getObjectMapper().readValue(r, SearchLookupRequest.class);
            System.out.println(req);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

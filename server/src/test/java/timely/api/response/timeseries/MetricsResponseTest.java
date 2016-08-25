package timely.api.response.timeseries;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import timely.Configuration;
import timely.api.model.Meta;
import timely.store.MetaCache;
import timely.store.MetaCacheFactory;
import timely.test.TestConfiguration;

public class MetricsResponseTest {

    public static class TestMetricsResponse extends MetricsResponse {

        public TestMetricsResponse(Configuration conf) {
            super(conf);
        }

        @Override
        public StringBuilder generateHtml() {
            return super.generateHtml();
        }

        @Override
        public String generateJson(ObjectMapper mapper) throws JsonProcessingException {
            return super.generateJson(mapper);
        }

    }

    @Test
    public void testGenerateHtml() throws Exception {
        Configuration cfg = TestConfiguration.createMinimalConfigurationForTest();
        MetaCache cache = MetaCacheFactory.getCache(cfg);
        cache.add(new Meta("sys.cpu.user", "host", "localhost"));
        cache.add(new Meta("sys.cpu.user", "instance", "0"));
        cache.add(new Meta("sys.cpu.idle", "host", "localhost"));
        cache.add(new Meta("sys.cpu.idle", "instance", "0"));
        TestMetricsResponse r = new TestMetricsResponse(cfg);
        String html = r.generateHtml().toString();
        Assert.assertTrue(html.contains("<td>sys.cpu.idle</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost instance=0 </td>"));
        Assert.assertTrue(html.contains("<td>sys.cpu.user</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost instance=0 </td>"));
    }

    @Test
    public void testGenerateHtmlWithIgnoredTags() throws Exception {
        Configuration cfg = TestConfiguration.createMinimalConfigurationForTest();
        cfg.getMetricsReportIgnoredTags().add("instance");
        MetaCache cache = MetaCacheFactory.getCache(cfg);
        cache.add(new Meta("sys.cpu.user", "host", "localhost"));
        cache.add(new Meta("sys.cpu.user", "instance", "0"));
        cache.add(new Meta("sys.cpu.idle", "host", "localhost"));
        cache.add(new Meta("sys.cpu.idle", "instance", "0"));
        TestMetricsResponse r = new TestMetricsResponse(cfg);
        String html = r.generateHtml().toString();
        Assert.assertTrue(html.contains("<td>sys.cpu.idle</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost </td>"));
        Assert.assertTrue(html.contains("<td>sys.cpu.user</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost </td>"));
    }

}

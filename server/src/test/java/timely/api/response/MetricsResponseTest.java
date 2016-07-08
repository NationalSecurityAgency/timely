package timely.api.response;

import java.io.File;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import timely.Configuration;
import timely.api.model.Meta;
import timely.api.response.MetricsResponse;
import timely.store.MetaCache;
import timely.store.MetaCacheFactory;
import timely.test.TestConfiguration;

public class MetricsResponseTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    private static File conf = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        temp.create();
    }

    @Test
    public void testGenerateHtml() throws Exception {
        conf = temp.newFile("config.properties");
        try {
            TestConfiguration cfg = TestConfiguration.createMinimalConfigurationForTest();
            cfg.toConfiguration(conf);
            Configuration config = new Configuration(conf);
            MetaCache cache = MetaCacheFactory.getCache(config);
            cache.add(new Meta("sys.cpu.user", "host", "localhost"));
            cache.add(new Meta("sys.cpu.user", "instance", "0"));
            cache.add(new Meta("sys.cpu.idle", "host", "localhost"));
            cache.add(new Meta("sys.cpu.idle", "instance", "0"));
            MetricsResponse r = new MetricsResponse();
            String html = r.generateHtml().toString();
            Assert.assertTrue(html.contains("<td>sys.cpu.idle</td>"));
            Assert.assertTrue(html.contains("<td>host=localhost instance=0 </td>"));
            Assert.assertTrue(html.contains("<td>sys.cpu.user</td>"));
            Assert.assertTrue(html.contains("<td>host=localhost instance=0 </td>"));
        } finally {
            conf.delete();
        }
    }

    @Test
    public void testGenerateHtmlWithIgnoredTags() throws Exception {
        conf = temp.newFile("config.properties");
        try {
            TestConfiguration cfg = TestConfiguration.createMinimalConfigurationForTest();
            cfg.put(Configuration.METRICS_IGNORED_TAGS, "instance");
            cfg.toConfiguration(conf);
            Configuration config = new Configuration(conf);
            MetaCache cache = MetaCacheFactory.getCache(config);
            cache.add(new Meta("sys.cpu.user", "host", "localhost"));
            cache.add(new Meta("sys.cpu.user", "instance", "0"));
            cache.add(new Meta("sys.cpu.idle", "host", "localhost"));
            cache.add(new Meta("sys.cpu.idle", "instance", "0"));
            MetricsResponse r = new MetricsResponse(config);
            String html = r.generateHtml().toString();
            Assert.assertTrue(html.contains("<td>sys.cpu.idle</td>"));
            Assert.assertTrue(html.contains("<td>host=localhost </td>"));
            Assert.assertTrue(html.contains("<td>sys.cpu.user</td>"));
            Assert.assertTrue(html.contains("<td>host=localhost </td>"));
        } finally {
            conf.delete();
        }
    }

}

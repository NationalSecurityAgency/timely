package timely.api.response.timeseries;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import timely.common.configuration.TimelyProperties;
import timely.model.Meta;
import timely.server.component.TestDataStore;
import timely.store.MetaCache;
import timely.test.IntegrationTest;
import timely.test.TimelyServerTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
public class MetricsResponseTestIT {

    @Autowired
    @Rule
    public TimelyServerTestRule testRule;

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private TimelyProperties timelyProperties;

    @Autowired
    private TestDataStore dataStore;

    @Before
    public void setup() {
        this.dataStore.reset();
    }

    @Test
    public void testGenerateHtml() {
        metaCache.add(new Meta("sys.cpu.user", "host", "localhost"));
        metaCache.add(new Meta("sys.cpu.user", "instance", "0"));
        metaCache.add(new Meta("sys.cpu.idle", "host", "localhost"));
        metaCache.add(new Meta("sys.cpu.idle", "instance", "0"));
        MetricsResponse response = new MetricsResponse(metaCache, timelyProperties);
        String html = response.generateHtml().toString();
        Assert.assertTrue(html.contains("<td>sys.cpu.idle</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost instance=0 </td>"));
        Assert.assertTrue(html.contains("<td>sys.cpu.user</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost instance=0 </td>"));
    }

    @Test
    public void testGenerateHtmlWithIgnoredTags() {
        timelyProperties.getMetricsReportIgnoredTags().add("instance");
        metaCache.add(new Meta("sys.cpu.user", "host", "localhost"));
        metaCache.add(new Meta("sys.cpu.user", "instance", "0"));
        metaCache.add(new Meta("sys.cpu.idle", "host", "localhost"));
        metaCache.add(new Meta("sys.cpu.idle", "instance", "0"));
        MetricsResponse response = new MetricsResponse(metaCache, timelyProperties);
        String html = response.generateHtml().toString();
        Assert.assertTrue(html.contains("<td>sys.cpu.idle</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost </td>"));
        Assert.assertTrue(html.contains("<td>sys.cpu.user</td>"));
        Assert.assertTrue(html.contains("<td>host=localhost </td>"));
    }
}

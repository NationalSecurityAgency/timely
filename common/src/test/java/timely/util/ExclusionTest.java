package timely.util;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

public class ExclusionTest {

    @Test
    public void testLoadFilteredMetricsFile() {
        Exclusions exclusions = new Exclusions();
        URL url = getClass().getClassLoader().getResource("filteredMetrics.txt");
        List<String> metricExclusions = exclusions.getFilteredMetrics(url.getFile());
        Assert.assertEquals(4, metricExclusions.size());
        System.out.println(metricExclusions);
    }

    @Test
    public void testLoadFilteredTagsFile() {
        Exclusions exclusions = new Exclusions();
        URL url = getClass().getClassLoader().getResource("filteredTags.txt");
        Map<String,Set<String>> filteredTags = exclusions.getFilteredTags(url.getFile());
        Assert.assertEquals(2, filteredTags.size());
        System.out.println(filteredTags);
    }

    @Test
    public void testMetricExclusions() {
        Exclusions exclusions = new Exclusions();
        URL filteredMetrics = getClass().getClassLoader().getResource("filteredMetrics.txt");
        URL filteredTags = getClass().getClassLoader().getResource("filteredTags.txt");
        exclusions.setFilteredMetricsFile(filteredMetrics.getFile());
        exclusions.setFilteredTagsFile(filteredTags.getFile());

        String metric1 = "put sys.interface.if_octets 1740090590221 71838.0 host=localhost instance=veth88545ad sampleType=DERIVE";
        Assert.assertTrue(exclusions.filterMetric(metric1));
        String metric2 = "put sys.interface.if_errors 1740090590221 71838.0 host=localhost instance=veth88545ad sampleType=DERIVE";
        Assert.assertFalse(exclusions.filterMetric(metric2));
        Assert.assertEquals("put sys.interface.if_octets 1740090590221 71838.0 host=localhost", exclusions.filterExcludedTags(metric1));
    }
}

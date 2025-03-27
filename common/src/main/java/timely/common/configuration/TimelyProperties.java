package timely.common.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.validation.constraints.Min;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.validation.annotation.Validated;

@RefreshScope
@Validated
@ConfigurationProperties(prefix = "timely")
public class TimelyProperties {

    private boolean test = false;
    private String metricsTable = "timely.metrics";
    private String metaTable = "timely.meta";
    private HashMap<String,Integer> metricAgeOffDays = new HashMap<>();
    private List<String> metricsReportIgnoredTags = new ArrayList<>();
    @Min(1)
    private int instance = 1;
    private int portIncrement = 100;
    private String defaultVisibility = null;
    private int tagFilterCacheSize = 10000;
    private String filteredMetricsFile = null;
    private String filteredTagsFile = null;

    public boolean isTest() {
        return test;
    }

    public void setTest(boolean test) {
        this.test = test;
    }

    public String getMetricsTable() {
        return metricsTable;
    }

    public TimelyProperties setMetricsTable(String metricsTable) {
        this.metricsTable = metricsTable;
        return this;
    }

    public String getMetaTable() {
        return metaTable;
    }

    public TimelyProperties setMetaTable(String metaTable) {
        this.metaTable = metaTable;
        return this;
    }

    public void setInstance(int instance) {
        this.instance = instance;
    }

    public int getInstance() {
        return instance;
    }

    public void setPortIncrement(int portIncrement) {
        this.portIncrement = portIncrement;
    }

    public int getPortIncrement() {
        return portIncrement;
    }

    public String getDefaultVisibility() {
        return defaultVisibility;
    }

    public void setDefaultVisibility(String defaultVisibility) {
        this.defaultVisibility = defaultVisibility;
    }

    public HashMap<String,Integer> getMetricAgeOffDays() {
        return metricAgeOffDays;
    }

    public void setMetricAgeOffDays(HashMap<String,Integer> metricAgeOffDays) {
        this.metricAgeOffDays = metricAgeOffDays;
    }

    public List<String> getMetricsReportIgnoredTags() {
        return metricsReportIgnoredTags;
    }

    public TimelyProperties setMetricsReportIgnoredTags(List<String> metricsReportIgnoredTags) {
        this.metricsReportIgnoredTags = metricsReportIgnoredTags;
        return this;
    }

    public void setTagFilterCacheSize(int tagFilterCacheSize) {
        this.tagFilterCacheSize = tagFilterCacheSize;
    }

    public int getTagFilterCacheSize() {
        return tagFilterCacheSize;
    }

    public void setFilteredMetricsFile(String filteredMetricsFile) {
        this.filteredMetricsFile = filteredMetricsFile;
    }

    public String getFilteredMetricsFile() {
        return filteredMetricsFile;
    }

    public void setFilteredTagsFile(String filteredTagsFile) {
        this.filteredTagsFile = filteredTagsFile;
    }

    public String getFilteredTagsFile() {
        return filteredTagsFile;
    }
}

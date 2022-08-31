package timely.common.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
    private String instance = null;
    private String defaultVisibility = null;

    public boolean getTest() {
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

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getInstance() {
        return instance;
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
}

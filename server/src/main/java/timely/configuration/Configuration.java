package timely.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.validation.Valid;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "timely")
public class Configuration {

    private String metricsTable = "timely.metrics";
    private String metaTable = "timely.meta";
    private HashMap<String, Integer> metricAgeOffDays = new HashMap<>();
    private List<String> metricsReportIgnoredTags = new ArrayList<>();
    private String instance = null;
    private String defaultVisibility = null;

    @Valid
    @NestedConfigurationProperty
    private Accumulo accumulo = new Accumulo();
    @Valid
    @NestedConfigurationProperty
    private Security security = new Security();
    @Valid
    @NestedConfigurationProperty
    private Server server = new Server();
    @Valid
    @NestedConfigurationProperty
    private Http http = new Http();
    @Valid
    @NestedConfigurationProperty
    private MetaCache metaCache = new MetaCache();
    @Valid
    @NestedConfigurationProperty
    private Cache cache = new Cache();
    @Valid
    @NestedConfigurationProperty
    private VisibilityCache visibilityCache = new VisibilityCache();
    @Valid
    @NestedConfigurationProperty
    private Websocket websocket = new Websocket();

    public String getMetricsTable() {
        return metricsTable;
    }

    public Configuration setMetricsTable(String metricsTable) {
        this.metricsTable = metricsTable;
        return this;
    }

    public String getMetaTable() {
        return metaTable;
    }

    public Configuration setMetaTable(String metaTable) {
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

    public HashMap<String, Integer> getMetricAgeOffDays() {
        return metricAgeOffDays;
    }

    public void setMetricAgeOffDays(HashMap<String, Integer> metricAgeOffDays) {
        this.metricAgeOffDays = metricAgeOffDays;
    }

    public List<String> getMetricsReportIgnoredTags() {
        return metricsReportIgnoredTags;
    }

    public Configuration setMetricsReportIgnoredTags(List<String> metricsReportIgnoredTags) {
        this.metricsReportIgnoredTags = metricsReportIgnoredTags;
        return this;
    }

    public Accumulo getAccumulo() {
        return accumulo;
    }

    public Security getSecurity() {
        return security;
    }

    public Server getServer() {
        return server;
    }

    public Http getHttp() {
        return http;
    }

    public Websocket getWebsocket() {
        return websocket;
    }

    public MetaCache getMetaCache() {
        return metaCache;
    }

    public Cache getCache() {
        return cache;
    }

    public VisibilityCache getVisibilityCache() {
        return visibilityCache;
    }
}

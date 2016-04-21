package timely.api.query.response;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.lang3.StringUtils;

import timely.Configuration;
import timely.api.model.Meta;
import timely.store.MetaCache;
import timely.store.MetaCacheFactory;

public class MetricsResponse {

    private static final String DOCTYPE = "<!DOCTYPE html>\n";
    private static final String META = "<meta charset=\"UTF-8\">\n";
    private static final String HTML_START = "<html>\n";
    private static final String HTML_END = "</html>\n";
    private static final String HEAD_START = "<head>\n";
    private static final String HEAD_END = "</head>\n";
    private static final String TITLE = "<title>Timely Metric Information</title>\n";
    private static final String BODY_START = "<body>\n";
    private static final String BODY_END = "</body>\n";
    private static final String HEADER_START = "<header>\n";
    private static final String HEADER_END = "</header>\n";
    private static final String HEADER_CONTENT = "<h2>Timely Metric Information</h2>\n<p>This page represents the metrics that Timely has in its internal cache, which may not be the entire set of available metrics depending on the configuration. The following tags are not shown here but are available for query: ";
    private static final String TABLE_START = "<table>\n";
    private static final String TABLE_END = "</table>\n";
    private static final String TR_START = "<tr>\n";
    private static final String TR_END = "</tr>\n";
    private static final String TH_START = "<th>";
    private static final String TH_END = "</th>\n";
    private static final String TD_START = "<td>";
    private static final String TD_END = "</td>\n";

    private Set<String> ignoredTags = Collections.emptySet();
    private Configuration conf = null;

    public MetricsResponse() {
    }

    public MetricsResponse(Configuration conf) {
        String tagsToIgnore = conf.get(Configuration.METRICS_IGNORED_TAGS);
        if (!StringUtils.isEmpty(tagsToIgnore)) {
            this.ignoredTags = new HashSet<>();
            for (String tag : tagsToIgnore.split(",")) {
                ignoredTags.add(tag);
            }
        }
        this.conf = conf;
    }

    public StringBuilder generateHtml() {
        final MetaCache cache = MetaCacheFactory.getCache(conf);
        TreeSet<Meta> tree = new TreeSet<>();
        cache.forEach(m -> tree.add(m));
        final StringBuilder b = new StringBuilder();
        b.append(DOCTYPE);
        b.append(HTML_START);
        b.append(HEAD_START);
        b.append(META);
        b.append(TITLE);
        b.append(HEAD_END);
        b.append(HEADER_START);
        b.append(HEADER_CONTENT).append(ignoredTags.toString()).append("</p>\n");
        b.append(HEADER_END);
        b.append(BODY_START);
        b.append(TABLE_START);
        b.append(TR_START);
        b.append(TH_START).append("Metric").append(TH_END);
        b.append(TH_START).append("Available Tags").append(TH_END);
        b.append(TR_END);
        String prevMetric = null;
        StringBuilder tags = new StringBuilder();
        for (Meta m : tree) {
            if (prevMetric != null && !m.getMetric().equals(prevMetric)) {
                b.append(TR_START);
                b.append(TD_START).append(prevMetric).append(TD_END);
                b.append(TD_START).append(tags.toString()).append(TD_END);
                b.append(TR_END);
                prevMetric = m.getMetric();
                tags.delete(0, tags.length());
            }
            if (prevMetric == null) {
                prevMetric = m.getMetric();
            }
            if (!(this.ignoredTags.contains(m.getTagKey()))) {
                tags.append(m.getTagKey()).append("=").append(m.getTagValue()).append(" ");
            }
        }
        b.append(TR_START);
        b.append(TD_START).append(prevMetric).append(TD_END);
        b.append(TD_START).append(tags.toString()).append(TD_END);
        b.append(TR_END);
        tags = null;
        prevMetric = null;
        b.append(TABLE_END);
        b.append(BODY_END);
        b.append(HTML_END);
        return b;
    }

}

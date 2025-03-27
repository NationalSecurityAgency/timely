package timely.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.model.Metric;
import timely.model.Tag;
import timely.model.parse.MetricParser;

public class Exclusions {

    private static final Logger log = LoggerFactory.getLogger(Exclusions.class);
    private static final MetricParser metricParser = new MetricParser();
    private List<String> filteredMetrics = new ArrayList<>();
    private Map<String,Set<String>> filteredTags = new LinkedHashMap<>();

    public interface LineHandler {
        void handleLine(String line);
    }

    public void readFile(String file, LineHandler lineHandler) {
        File f = new File(file);
        if (f.exists() && f.isFile() && f.canRead()) {
            try (InputStream iStream = new FileInputStream(f);
                            InputStreamReader streamReader = new InputStreamReader(iStream, StandardCharsets.UTF_8);
                            BufferedReader reader = new BufferedReader(streamReader)) {

                String line;
                while ((line = reader.readLine()) != null) {
                    try {
                        lineHandler.handleLine(line);
                    } catch (Exception e) {
                        log.error(MessageFormat.format("Error reading line {0}", line), e);
                    }
                }
            } catch (IOException e) {
                log.error(MessageFormat.format("Error reading file {0}", file), e);
            }
        } else {
            log.error(MessageFormat.format("File ''{0}'' either does not exist or is not readable", file));
        }
    }

    public List<String> getFilteredMetrics(String filteredMetricsFile) {
        List<String> filteredMetricRegexes = new ArrayList<>();
        readFile(filteredMetricsFile, (line) -> {
            if (!line.endsWith(".*")) {
                line = line + ".*";
            }
            filteredMetricRegexes.add(line);
        });
        return filteredMetricRegexes;
    }

    public Map<String,Set<String>> getFilteredTags(String filteredTagsFile) {
        Map<String,Set<String>> filteredTagsForMetrics = new LinkedHashMap<>();
        readFile(filteredTagsFile, (line) -> {
            int x = line.indexOf(" ");
            String metric = line.substring(0, x);
            String tags = line.substring(x + 1).trim();
            Set<String> tagSet = Arrays.stream(tags.split(",")).map(tag -> tag.trim()).collect(Collectors.toSet());
            filteredTagsForMetrics.put(metric, tagSet);
        });
        return filteredTagsForMetrics;
    }

    public void setFilteredMetricsFile(String filteredMetricsFile) {
        this.filteredMetrics = getFilteredMetrics(filteredMetricsFile);
    }

    public void setFilteredTagsFile(String filteredTagsFile) {
        this.filteredTags = getFilteredTags(filteredTagsFile);
    }

    // format put metric timestamp value tag1=value1 tag2=value2 tag3=value3
    // where the tags are sorted
    public boolean filterMetric(String metricPutLine) {
        String metricLine = buildMetricLine(metricPutLine);
        for (String filteredRegex : filteredMetrics) {
            if (metricLine.matches(filteredRegex)) {
                return true;
            }
        }
        return false;
    }

    public boolean filterMetric(Metric metric) {
        String metricLine = buildMetricLine(metric);
        for (String filteredRegex : filteredMetrics) {
            if (metricLine.matches(filteredRegex)) {
                return true;
            }
        }
        return false;
    }

    protected String buildMetricLine(String metricPutLine) {
        Metric metric = metricParser.parse(metricPutLine);
        return buildMetricLine(metric);
    }

    protected String buildMetricLine(Metric metric) {
        StringBuilder sb = new StringBuilder();
        sb.append(metric.getName()).append(" ");
        metric.getTags().stream().forEach(t -> sb.append(t.join()).append(" "));
        return sb.toString().trim();
    }

    public String filterExcludedTags(String metricLineOrig) {
        if (filteredTags.isEmpty()) {
            return metricLineOrig;
        }
        Metric metric = metricParser.parse(metricLineOrig);
        filterExcludedTags(metric);
        StringBuilder sb = new StringBuilder();
        sb.append("put ");
        sb.append(metric.getName()).append(" ");
        sb.append(metric.getValue().getTimestamp()).append(" ");
        sb.append(metric.getValue().getMeasure()).append(" ");
        metric.getTags().stream().forEach(t -> sb.append(t.join()).append(" "));
        return sb.toString().trim();
    }

    public void filterExcludedTags(Metric metric) {
        if (!filteredTags.isEmpty()) {
            Set<String> tags = filteredTags.get(metric.getName());
            if (tags != null && !tags.isEmpty()) {
                List<Tag> filteredTags = metric.getTags().stream().filter(t -> !tags.contains(t.getKey())).collect(Collectors.toList());
                metric.setTags(filteredTags);
            }
        }
    }
}

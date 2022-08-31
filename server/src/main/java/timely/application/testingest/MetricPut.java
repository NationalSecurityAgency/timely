package timely.application.testingest;

import java.util.Map;
import java.util.Random;

public class MetricPut {

    private String metric;
    private Map<String,String> tags;
    private Random r = new Random();

    public MetricPut(String metric, Map<String,String> tags) {
        this.metric = metric;
        this.tags = tags;
    }

    public String formatTcp(long timestamp, Double value) {

        StringBuilder sb = new StringBuilder();
        sb.append("put ");
        sb.append(metric).append(" ");
        sb.append(timestamp).append(" ");
        sb.append(value);
        tags.entrySet().stream().forEach(e -> sb.append(String.format(" %s=%s", e.getKey(), e.getValue())));
        sb.append("\n");
        return sb.toString();
    }

    public String generateTcpLine(long timestamp) {
        double value = r.nextInt(1000) / 1.0;
        return formatTcp(timestamp, value);
    }
}

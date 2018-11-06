package timely.testing;

import java.util.Map;
import java.util.Random;

public class MetricPut {

    private String metric = null;
    private Map<String, String> tags;
    private Random r = new Random();

    public MetricPut(String metric, Map<String, String> tags) {
        this.metric = metric;
        this.tags = tags;
    }

    public String formatTcp(long timestamp, Double value) {

        StringBuilder sb = new StringBuilder();
        sb.append("put ");
        sb.append(metric).append(" ");
        sb.append(Long.toString(timestamp)).append(" ");
        sb.append(Double.toString(value));
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            sb.append(" ");
            sb.append(entry.getKey());
            sb.append("=");
            sb.append(entry.getValue());
        }
        sb.append("\n");
        return sb.toString();
    }

    public String generateTcpLine(long timestamp) {
        double value = r.nextInt(1000) / 1.0;
        return formatTcp(timestamp, value);
    }
}

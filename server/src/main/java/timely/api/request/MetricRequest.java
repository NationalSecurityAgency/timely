package timely.api.request;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import timely.api.annotation.Http;
import timely.api.annotation.Tcp;
import timely.api.annotation.Udp;
import timely.api.annotation.WebSocket;
import timely.model.Metric;
import timely.model.parse.MetricParser;
import timely.util.JsonUtil;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.nio.charset.StandardCharsets;

@Tcp(operation = "put")
@Udp(operation = "put")
@Http(path = "/api/put")
@WebSocket(operation = "put")
@XmlRootElement(name = "metric")
public class MetricRequest implements TcpRequest, HttpPostRequest, WebSocketRequest, UdpRequest {

    private static final Logger LOG = LoggerFactory.getLogger(MetricRequest.class);

    @XmlElement
    @JsonUnwrapped
    private Metric metric;

    private static final MetricParser metricParser = new MetricParser();

    public MetricRequest() {
    }

    public MetricRequest(Metric metric) {
        this.metric = metric;
    }

    public Metric getMetric() {
        return metric;
    }

    public void setMetric(Metric metric) {
        this.metric = metric;
    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        metric = JsonUtil.getObjectMapper().readValue(content.getBytes(StandardCharsets.UTF_8), Metric.class);
        return this;
    }

    @Override
    public void parse(String line) {
        LOG.trace("Parsing Line: {}", line);
        metric = metricParser.parse(line);
    }

    @Override
    public String toString() {
        return metric.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        MetricRequest that = (MetricRequest) o;

        return metric != null ? metric.equals(that.metric) : that.metric == null;

    }

    @Override
    public int hashCode() {
        return metric != null ? metric.hashCode() : 0;
    }
}

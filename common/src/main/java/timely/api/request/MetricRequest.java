package timely.api.request;

import java.nio.charset.StandardCharsets;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.netty.handler.codec.http.FullHttpRequest;
import timely.api.annotation.Http;
import timely.api.annotation.Tcp;
import timely.api.annotation.Udp;
import timely.api.annotation.WebSocket;
import timely.api.request.websocket.WebSocketRequest;
import timely.model.Metric;
import timely.model.parse.MetricParser;
import timely.util.JsonUtil;

@Tcp(operation = "put")
@Udp(operation = "put")
@Http(path = "/api/put")
@WebSocket(operation = "put")
@XmlRootElement(name = "metric")
public class MetricRequest implements TcpRequest, HttpPostRequest, WebSocketRequest, UdpRequest {

    private static final Logger log = LoggerFactory.getLogger(MetricRequest.class);
    private FullHttpRequest httpRequest = null;

    @XmlElement
    @JsonUnwrapped
    private Metric metric;

    private String line;

    private static final MetricParser metricParser = new MetricParser();

    public MetricRequest() {}

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
        log.trace("Parsing Line: {}", line);
        try {
            this.metric = metricParser.parse(line);
            this.line = line;
        } catch (Exception e) {
            log.error("Error parsing metric: {}", line);
            throw e;
        }
    }

    @Override
    public String toString() {
        return metric.toString();
    }

    public String getLine() {
        return line;
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

    public void setHttpRequest(FullHttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public FullHttpRequest getHttpRequest() {
        return httpRequest;
    }
}

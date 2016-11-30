package timely.api.response;

import java.util.ArrayList;
import java.util.List;

public class MetricResponses {

    private List<MetricResponse> responses = new ArrayList<>();

    public synchronized List<MetricResponse> getResponses() {
        return this.responses;
    }

    public synchronized void setResponses(List<MetricResponse> responses) {
        this.responses = responses;
    }

    public synchronized void addResponse(MetricResponse r) {
        this.responses.add(r);
    }

    public int size() {
        return this.responses.size();
    }

    public synchronized void clear() {
        this.responses.clear();
    }
}

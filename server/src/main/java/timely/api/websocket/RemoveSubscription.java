package timely.api.websocket;

public class RemoveSubscription extends WSRequest {

    private String metric = null;

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

}

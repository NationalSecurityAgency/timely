package timely.api.request;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "remove")
public class RemoveSubscription extends AuthenticatedWebSocketRequest {

    private String metric = null;

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

}

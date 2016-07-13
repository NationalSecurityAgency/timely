package timely.api.request.subscription;

import timely.api.annotation.WebSocket;
import timely.api.request.AuthenticatedWebSocketRequest;

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

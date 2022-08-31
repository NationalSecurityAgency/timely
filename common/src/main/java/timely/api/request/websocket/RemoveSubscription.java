package timely.api.request.websocket;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "remove")
public class RemoveSubscription extends SubscriptionRequest {

    private String metric = null;

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

}

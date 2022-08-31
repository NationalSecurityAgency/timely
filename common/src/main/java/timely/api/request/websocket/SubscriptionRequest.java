package timely.api.request.websocket;

import timely.api.request.AuthenticatedWebSocketRequest;

public class SubscriptionRequest extends AuthenticatedWebSocketRequest {

    private String subscriptionId = null;

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

    @Override
    public void validate() {
        if (null == subscriptionId) {
            throw new IllegalArgumentException("Subscription ID is required.");
        }
    }

}

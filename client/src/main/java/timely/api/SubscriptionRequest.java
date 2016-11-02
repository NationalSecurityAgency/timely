package timely.api;

public abstract class SubscriptionRequest {

    protected String subscriptionId = null;

    public final String getSubscriptionId() {
        return subscriptionId;
    }

    public final void setSubscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
    }

}

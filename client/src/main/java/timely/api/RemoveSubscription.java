package timely.api;

public class RemoveSubscription extends SubscriptionRequest {

    private String operation = "remove";
    private String metric = null;

    public String getOperation() {
        return operation;
    }

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    @Override
    public int hashCode() {
        return (operation + subscriptionId + metric).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof RemoveSubscription) {
            RemoveSubscription other = (RemoveSubscription) obj;
            return (this.subscriptionId.equals(other.subscriptionId) && this.metric.equals(other.metric));
        } else {
            return false;
        }
    }

}

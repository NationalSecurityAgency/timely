package timely.api;

public class CloseSubscription extends SubscriptionRequest {

    private String operation = "close";

    public String getOperation() {
        return operation;
    }

    @Override
    public int hashCode() {
        return (operation + subscriptionId).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof CloseSubscription) {
            CloseSubscription other = (CloseSubscription) obj;
            return (this.subscriptionId.equals(other.subscriptionId));
        } else {
            return false;
        }
    }

}

package timely.api;

public class CreateSubscription extends SubscriptionRequest {

    private String operation = "create";

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
        if (obj instanceof CreateSubscription) {
            CreateSubscription other = (CreateSubscription) obj;
            return (this.subscriptionId.equals(other.subscriptionId));
        } else {
            return false;
        }
    }

}

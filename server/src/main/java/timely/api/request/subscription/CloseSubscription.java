package timely.api.request.subscription;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "close")
public class CloseSubscription extends SubscriptionRequest {
}

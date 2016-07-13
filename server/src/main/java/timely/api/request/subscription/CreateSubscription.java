package timely.api.request.subscription;

import timely.api.annotation.WebSocket;
import timely.api.request.AuthenticatedWebSocketRequest;

@WebSocket(operation = "create")
public class CreateSubscription extends AuthenticatedWebSocketRequest {
}

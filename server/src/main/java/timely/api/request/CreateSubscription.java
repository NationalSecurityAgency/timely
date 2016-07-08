package timely.api.request;

import timely.api.annotation.WebSocket;

@WebSocket(operation = "create")
public class CreateSubscription extends AuthenticatedWebSocketRequest {
}

package timely.api.request.subscription;

import timely.api.annotation.WebSocket;
import timely.api.request.AuthenticatedWebSocketRequest;

@WebSocket(operation = "close")
public class CloseSubscription extends AuthenticatedWebSocketRequest {
}

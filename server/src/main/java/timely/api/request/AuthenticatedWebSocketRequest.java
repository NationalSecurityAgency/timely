package timely.api.request;

import io.netty.handler.codec.http.FullHttpRequest;

public class AuthenticatedWebSocketRequest extends AuthenticatedRequest implements WebSocketRequest {

    private FullHttpRequest httpRequest = null;

    public void setHttpRequest(FullHttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public FullHttpRequest getHttpRequest() {
        return httpRequest;
    }
}

package timely.api.request;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;
import timely.api.annotation.WebSocket;

@Http(path = "/api/cache")
@WebSocket(operation = "cache")
public class CacheRequest extends AuthenticatedRequest implements HttpGetRequest, HttpPostRequest, WebSocketRequest {

    private FullHttpRequest httpRequest = null;

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        return new CacheRequest();
    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        return new CacheRequest();
    }

    public void setHttpRequest(FullHttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public FullHttpRequest getHttpRequest() {
        return httpRequest;
    }
}

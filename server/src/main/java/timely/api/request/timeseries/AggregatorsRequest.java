package timely.api.request.timeseries;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;
import timely.api.annotation.WebSocket;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.WebSocketRequest;

@Http(path = "/api/aggregators")
@WebSocket(operation = "aggregators")
public class AggregatorsRequest extends AuthenticatedRequest implements HttpGetRequest, HttpPostRequest,
        WebSocketRequest {

    private FullHttpRequest httpRequest = null;

    @Override
    public String toString() {
        return "";
    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        return new AggregatorsRequest();
    }

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        return new AggregatorsRequest();
    }

    public void setHttpRequest(FullHttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public FullHttpRequest getHttpRequest() {
        return httpRequest;
    }
}

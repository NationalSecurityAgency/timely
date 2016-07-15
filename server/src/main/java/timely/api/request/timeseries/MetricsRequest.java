package timely.api.request.timeseries;

import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;
import timely.api.annotation.WebSocket;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.WebSocketRequest;

@Http(path = "/api/metrics")
@WebSocket(operation = "metrics")
public class MetricsRequest extends AuthenticatedRequest implements HttpGetRequest, HttpPostRequest, WebSocketRequest {

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        return new MetricsRequest();
    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        return new MetricsRequest();
    }

}

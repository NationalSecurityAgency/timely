package timely.api.query.request;

import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;

@Http(path = "/api/metrics")
public class MetricsRequest extends AuthenticatedRequest implements HttpGetRequest, HttpPostRequest {

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        return new MetricsRequest();
    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        return new MetricsRequest();
    }

}

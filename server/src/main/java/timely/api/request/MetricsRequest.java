package timely.api.request;

import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;

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

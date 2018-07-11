package timely.api.request;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.request.timeseries.HttpRequest;

public interface HttpGetRequest extends HttpRequest {

    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception;

    public void setHttpRequest(FullHttpRequest httpRequest);

    public FullHttpRequest getHttpRequest();

}

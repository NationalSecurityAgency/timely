package timely.api.request;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;

public interface HttpGetRequest extends HttpRequest {

    HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception;

    void setHttpRequest(FullHttpRequest httpRequest);

    FullHttpRequest getHttpRequest();

}

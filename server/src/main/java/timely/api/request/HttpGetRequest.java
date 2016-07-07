package timely.api.request;

import io.netty.handler.codec.http.QueryStringDecoder;

public interface HttpGetRequest extends Request {

    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception;

}

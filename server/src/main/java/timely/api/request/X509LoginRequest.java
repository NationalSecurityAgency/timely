package timely.api.request;

import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;

@Http(path = "/login")
public class X509LoginRequest implements HttpGetRequest {

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        return new X509LoginRequest();
    }

}

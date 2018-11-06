package timely.api.request.auth;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;
import timely.api.request.HttpGetRequest;

@Http(path = "/login")
public class X509LoginRequest implements HttpGetRequest {

    private FullHttpRequest httpRequest = null;

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        return new X509LoginRequest();
    }

    public void setHttpRequest(FullHttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public FullHttpRequest getHttpRequest() {
        return httpRequest;
    }
}

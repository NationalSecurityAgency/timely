package timely.api.request.auth;

import io.netty.handler.codec.http.FullHttpRequest;
import timely.api.BasicAuthLogin;
import timely.api.annotation.Http;
import timely.api.request.HttpPostRequest;
import timely.util.JsonUtil;

@Http(path = "/login")
public class BasicAuthLoginRequest extends BasicAuthLogin implements HttpPostRequest {

    private FullHttpRequest httpRequest = null;

    @Override
    public void validate() {
        HttpPostRequest.super.validate();
        if (username == null || password == null) {
            throw new IllegalArgumentException("Username and password are required.");
        }
    }

    @Override
    public String toString() {
        return "Username: " + username;
    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        return JsonUtil.getObjectMapper().readValue(content, BasicAuthLoginRequest.class);
    }

    public void setHttpRequest(FullHttpRequest httpRequest) {
        this.httpRequest = httpRequest;
    }

    public FullHttpRequest getHttpRequest() {
        return httpRequest;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }
}

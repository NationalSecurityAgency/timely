package timely.api.request.auth;

import timely.api.BasicAuthLogin;
import timely.api.annotation.Http;
import timely.api.request.HttpPostRequest;
import timely.util.JsonUtil;

@Http(path = "/login")
public class BasicAuthLoginRequest extends BasicAuthLogin implements HttpPostRequest {

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

}

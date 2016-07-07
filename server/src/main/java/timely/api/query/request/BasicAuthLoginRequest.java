package timely.api.query.request;

import timely.api.annotation.Http;
import timely.api.request.HttpPostRequest;
import timely.util.JsonUtil;

@Http(path = "/api/login")
public class BasicAuthLoginRequest implements HttpPostRequest {

    private String username;
    private String password;

    public String getUsername() {
        return username;
    }

    public void setUsername(String user) {
        this.username = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

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

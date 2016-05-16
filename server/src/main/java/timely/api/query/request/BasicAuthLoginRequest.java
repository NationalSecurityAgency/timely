package timely.api.query.request;

import timely.api.Request;

public class BasicAuthLoginRequest implements Request {

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
        Request.super.validate();
        if (username == null || password == null) {
            throw new IllegalArgumentException("Username and password are required.");
        }
    }

    @Override
    public String toString() {
        return "Username: " + username;
    }

}

package timely.api;

public class BasicAuthLogin {

    protected String username;
    protected String password;

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
    public int hashCode() {
        return (username + password).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj instanceof BasicAuthLogin) {
            BasicAuthLogin other = (BasicAuthLogin) obj;
            return (this.username.equals(other.username) && this.password.equals(other.password));
        } else {
            return false;
        }
    }

}

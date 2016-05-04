package timely.api;

public class AuthenticatedRequest implements Request {

    private String sessionId = null;

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public void validate() {
        Request.super.validate();
        if (null == sessionId) {
            throw new IllegalArgumentException("SessionID is null, must log in first");
        }
    }

}

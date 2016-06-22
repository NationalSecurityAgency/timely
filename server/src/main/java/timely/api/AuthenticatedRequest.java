package timely.api;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class AuthenticatedRequest implements Request {

    private String sessionId = null;
    private Map<String, String> requestHeaders = new HashMap<>();

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void addHeaders(List<Entry<String, String>> headers) {
        headers.forEach(l -> requestHeaders.put(l.getKey(), l.getValue()));
    }

    public Map<String, String> getRequestHeaders() {
        return Collections.unmodifiableMap(requestHeaders);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("{");
        buf.append("Session ID: ").append(sessionId);
        buf.append(", Request Headers: ").append(requestHeaders.toString());
        buf.append("}");
        return buf.toString();
    }

    @Override
    public void validate() {
        Request.super.validate();
        if (null == sessionId) {
            throw new IllegalArgumentException("SessionID is null, must log in first");
        }
    }

}

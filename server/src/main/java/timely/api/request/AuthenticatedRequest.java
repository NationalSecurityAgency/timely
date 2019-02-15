package timely.api.request;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import timely.auth.TimelyUser;
import timely.auth.util.HttpHeaderUtils;
import timely.netty.http.auth.TimelyAuthenticationToken;

/**
 * Base class for requests that require auth
 */
@JsonIgnoreProperties({ "userName", "requestHeader" })
public class AuthenticatedRequest implements Request {

    private String sessionId = null;
    private TimelyAuthenticationToken token = null;
    private Multimap<String, String> requestHeaders = HashMultimap.create();

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public void addHeaders(Multimap<String, String> headers) {
        requestHeaders.putAll(headers);
    }

    public Multimap<String, String> getRequestHeaders() {
        return requestHeaders;
    }

    @JsonIgnore
    public String getRequestHeader(String name) {
        return HttpHeaderUtils.getSingleHeader(requestHeaders, name, false);
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("{");
        buf.append("Session ID: ").append(sessionId);
        buf.append("Token: ").append(token == null ? null : token.getTimelyPrincipal().getName());
        buf.append(", Request Headers: ").append(requestHeaders.toString());
        buf.append("}");
        return buf.toString();
    }

    public void setToken(TimelyAuthenticationToken token) {
        this.token = token;
    }

    public TimelyAuthenticationToken getToken() {
        return token;
    }

    @JsonIgnore
    public String getUserName() {
        String name;
        try {
            TimelyUser user = token.getTimelyPrincipal().getPrimaryUser();
            name = user.getCommonName();
            if (name == null) {
                name = user.getName();
            }
        } catch (Exception e) {
            name = "UNKNOWN";
        }
        return name;
    }
}

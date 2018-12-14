package timely.configuration;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;

public class Cors {

    private boolean allowAnyOrigin = false;
    private boolean allowNullOrigin = false;
    private Set<String> allowedOrigins = new HashSet<>();
    private List<String> allowedMethods = Lists.newArrayList("DELETE", "GET", "HEAD", "OPTIONS", "PUT", "POST");
    private List<String> allowedHeaders = Lists.newArrayList("content-type");
    private boolean allowCredentials = true;

    public boolean isAllowAnyOrigin() {
        return allowAnyOrigin;
    }

    public void setAllowAnyOrigin(boolean allowAnyOrigin) {
        this.allowAnyOrigin = allowAnyOrigin;
    }

    public boolean isAllowNullOrigin() {
        return allowNullOrigin;
    }

    public void setAllowNullOrigin(boolean allowNullOrigin) {
        this.allowNullOrigin = allowNullOrigin;
    }

    public Set<String> getAllowedOrigins() {
        return allowedOrigins;
    }

    public void setAllowedOrigins(Set<String> allowedOrigins) {
        this.allowedOrigins = allowedOrigins;
    }

    public List<String> getAllowedMethods() {
        return allowedMethods;
    }

    public void setAllowedMethods(List<String> allowedMethods) {
        this.allowedMethods = allowedMethods;
    }

    public List<String> getAllowedHeaders() {
        return allowedHeaders;
    }

    public void setAllowedHeaders(List<String> allowedHeaders) {
        this.allowedHeaders = allowedHeaders;
    }

    public boolean isAllowCredentials() {
        return allowCredentials;
    }

    public void setAllowCredentials(boolean allowCredentials) {
        this.allowCredentials = allowCredentials;
    }
}

package timely.common.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.security.authorization.remote")
public class AuthorizationProperties {

    private boolean enabled = false;
    private String authorizationUrl;
    private String publicCertificateFile;
    private String publicCertificateType;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getAuthorizationUrl() {
        return authorizationUrl;
    }

    public void setAuthorizationUrl(String authorizationUrl) {
        this.authorizationUrl = authorizationUrl;
    }

    public String getPublicCertificateFile() {
        return publicCertificateFile;
    }

    public void setPublicCertificateFile(String publicCertificateFile) {
        this.publicCertificateFile = publicCertificateFile;
    }

    public String getPublicCertificateType() {
        return publicCertificateType;
    }

    public void setPublicCertificateType(String publicCertificateType) {
        this.publicCertificateType = publicCertificateType;
    }
}

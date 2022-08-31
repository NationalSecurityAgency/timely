package timely.common.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

@RefreshScope
@ConfigurationProperties(prefix = "timely.security.client-ssl")
public class SslClientProperties implements SslProperties {

    private String[] ciphers = new String[] {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_RSA_WITH_AES_128_CBC_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA"};
    private String[] enabledProtocols;
    private String keyStoreType;
    private String keyStoreFile;
    private String keyStorePassword;
    private String keyAlias;
    private String trustStoreType;
    private String trustStoreFile;
    private String trustStorePassword;
    private boolean useClientCert = true;
    private boolean hostVerificationEnabled = true;
    private boolean useOpenssl = true;

    public String[] getCiphers() {
        return ciphers;
    }

    public void setCiphers(String[] ciphers) {
        this.ciphers = ciphers;
    }

    public String[] getEnabledProtocols() {
        return enabledProtocols;
    }

    public void setEnabledProtocols(String[] enabledProtocols) {
        this.enabledProtocols = enabledProtocols;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public String getKeyStoreFile() {
        return keyStoreFile;
    }

    public void setKeyStoreFile(String keyStoreFile) {
        this.keyStoreFile = keyStoreFile;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getKeyAlias() {
        return keyAlias;
    }

    public void setKeyAlias(String keyAlias) {
        this.keyAlias = keyAlias;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public String getTrustStoreFile() {
        return trustStoreFile;
    }

    public void setTrustStoreFile(String trustStoreFile) {
        this.trustStoreFile = trustStoreFile;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public void setUseClientCert(boolean useClientCert) {
        this.useClientCert = useClientCert;
    }

    public boolean isUseClientCert() {
        return useClientCert;
    }

    public void setHostVerificationEnabled(boolean hostVerificationEnabled) {
        this.hostVerificationEnabled = hostVerificationEnabled;
    }

    public boolean isHostVerificationEnabled() {
        return hostVerificationEnabled;
    }

    public boolean isUseOpenssl() {
        return useOpenssl;
    }

    public void setUseOpenssl(boolean useOpenssl) {
        this.useOpenssl = useOpenssl;
    }
}

package timely.common.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import timely.validator.NotEmptyIfFieldSet;

@RefreshScope
@NotEmptyIfFieldSet.List({@NotEmptyIfFieldSet(fieldName = "useGeneratedKeypair", fieldValue = "false", notNullFieldName = "keyStoreFile",
                message = "must be set if timely.security.ssl.use-generated-keypair is false")})
@ConfigurationProperties(prefix = "timely.security.server-ssl")
public class SslServerProperties implements SslProperties {

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
    private boolean useGeneratedKeypair = false;
    private boolean ignoreSslHandshakeErrors = false;
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

    public boolean isUseGeneratedKeypair() {
        return useGeneratedKeypair;
    }

    public void setUseGeneratedKeypair(boolean useGeneratedKeypair) {
        this.useGeneratedKeypair = useGeneratedKeypair;
    }

    public boolean isIgnoreSslHandshakeErrors() {
        return ignoreSslHandshakeErrors;
    }

    public void setIgnoreSslHandshakeErrors(boolean ignoreSslHandshakeErrors) {
        this.ignoreSslHandshakeErrors = ignoreSslHandshakeErrors;
    }

    public boolean isUseOpenssl() {
        return useOpenssl;
    }

    public void setUseOpenssl(boolean useOpenssl) {
        this.useOpenssl = useOpenssl;
    }
}

package timely.common.configuration;

import java.net.URL;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.util.ResourceUtils;

import timely.validator.NotEmptyIfFieldSet;

@RefreshScope
@NotEmptyIfFieldSet.List({@NotEmptyIfFieldSet(fieldName = "useGeneratedKeypair", fieldValue = "false", notNullFieldName = "keyStoreFile",
                message = "must be set if timely.security.ssl.use-generated-keypair is false")})
@ConfigurationProperties(prefix = "timely.security.server-ssl")
public class SslServerProperties implements SslProperties {

    private static final Logger log = LoggerFactory.getLogger(SslServerProperties.class);

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

    @PostConstruct
    public void setTrustStoreSystemProperties() {
        try {
            if (StringUtils.isNotBlank(trustStoreFile)) {
                // This step is to resolve any classpath: prefixes used in trustStoreFile
                URL url = ResourceUtils.getURL(trustStoreFile);
                if (url != null) {
                    System.setProperty("javax.net.ssl.trustStore", url.getFile());
                    if (StringUtils.isNotBlank(trustStoreType)) {
                        System.setProperty("javax.net.ssl.trustStoreType", trustStoreType);
                    }
                    if (StringUtils.isNotBlank(trustStorePassword)) {
                        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
                    }
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

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

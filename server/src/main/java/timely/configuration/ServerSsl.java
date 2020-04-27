package timely.configuration;

import java.util.List;
import java.util.Objects;

import com.google.common.collect.Lists;
import timely.validator.NotEmptyIfFieldSet;

@NotEmptyIfFieldSet.List({
        @NotEmptyIfFieldSet(fieldName = "useGeneratedKeypair", fieldValue = "false", notNullFieldName = "certificateFile", message = "must be set if timely.security.ssl.use-generated-keypair is false"),
        @NotEmptyIfFieldSet(fieldName = "useGeneratedKeypair", fieldValue = "false", notNullFieldName = "keyFile", message = "must be set if timely.security.ssl.use-generated-keypair is false") })

public class ServerSsl {

    private String certificateFile;
    private String keyFile;
    private String keyPassword;
    private String trustStoreFile;
    private boolean useGeneratedKeypair = false;
    private boolean useOpenssl = true;
    private List<String> useCiphers = Lists.newArrayList("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_GCM_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA",
            "SSL_RSA_WITH_3DES_EDE_CBC_SHA");

    public String getCertificateFile() {
        return certificateFile;
    }

    public void setCertificateFile(String certificateFile) {
        this.certificateFile = certificateFile;
    }

    public String getKeyFile() {
        return keyFile;
    }

    public void setKeyFile(String keyFile) {
        this.keyFile = keyFile;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public String getTrustStoreFile() {
        return trustStoreFile;
    }

    public void setTrustStoreFile(String trustStoreFile) {
        this.trustStoreFile = trustStoreFile;
    }

    public boolean isUseGeneratedKeypair() {
        return useGeneratedKeypair;
    }

    public void setUseGeneratedKeypair(boolean useGeneratedKeypair) {
        this.useGeneratedKeypair = useGeneratedKeypair;
    }

    public boolean isUseOpenssl() {
        return useOpenssl;
    }

    public void setUseOpenssl(boolean useOpenssl) {
        this.useOpenssl = useOpenssl;
    }

    public List<String> getUseCiphers() {
        return useCiphers;
    }

    public void setUseCiphers(List<String> useCiphers) {
        this.useCiphers = useCiphers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ServerSsl serverSsl = (ServerSsl) o;
        return useGeneratedKeypair == serverSsl.useGeneratedKeypair && useOpenssl == serverSsl.useOpenssl
                && Objects.equals(certificateFile, serverSsl.certificateFile)
                && Objects.equals(keyFile, serverSsl.keyFile) && Objects.equals(keyPassword, serverSsl.keyPassword)
                && Objects.equals(trustStoreFile, serverSsl.trustStoreFile)
                && Objects.equals(useCiphers, serverSsl.useCiphers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(certificateFile, keyFile, keyPassword, trustStoreFile, useGeneratedKeypair, useOpenssl,
                useCiphers);
    }
}

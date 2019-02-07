package timely.balancer.configuration;

public class ClientSsl {

    private String keyFile;
    private String keyType;
    private String keyPassword;
    private String trustStoreFile;
    private String trustStoreType;
    private String trustStorePassword;
    private boolean useClientCert = true;
    private boolean hostVerificationEnabled = true;

    public String getKeyFile() {
        return keyFile;
    }

    public void setKeyFile(String keyFile) {
        this.keyFile = keyFile;
    }

    public String getKeyType() {
        return keyType;
    }

    public void setKeyType(String keyType) {
        this.keyType = keyType;
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

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
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
}

package timely.common.configuration;

public interface SslProperties {

    String getKeyStoreType();

    void setKeyStoreType(String keyStoreType);

    String getKeyStoreFile();

    void setKeyStoreFile(String keyStoreFile);

    String getKeyStorePassword();

    void setKeyStorePassword(String keyStorePassword);

    String getKeyAlias();

    void setKeyAlias(String keyAlias);

    String getTrustStoreType();

    void setTrustStoreType(String trustStoreType);

    String getTrustStoreFile();

    void setTrustStoreFile(String trustStoreFile);

    String getTrustStorePassword();

    void setTrustStorePassword(String trustStorePassword);

    String[] getCiphers();

    void setCiphers(String[] ciphers);

    String[] getEnabledProtocols();

    void setEnabledProtocols(String[] enabledProtocols);

    boolean isUseOpenssl();

    void setUseOpenssl(boolean useOpenssl);

}

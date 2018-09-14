package timely.balancer;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;

import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;
import timely.balancer.connection.TimelyBalancedHost;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "timely.balancer")
public class BalancerConfiguration {

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig udpClientPool = new GenericKeyedObjectPoolConfig();

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig tcpClientPool = new GenericKeyedObjectPoolConfig();

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig httpClientPool = new GenericKeyedObjectPoolConfig();

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig wsClientPool = new GenericKeyedObjectPoolConfig();

    @Valid
    @NestedConfigurationProperty
    private ClientSsl clientSsl = new ClientSsl();

    private boolean loginRequired = false;

    @Valid
    @NestedConfigurationProperty
    private List<TimelyBalancedHost> timelyHosts = new ArrayList<>();

    private long checkServerHealthInterval = 10000;

    private int serverFailuresBeforeDown = 3;

    private int serverSuccessesBeforeUp = 3;

    @Valid
    @NestedConfigurationProperty
    private List<TimelyBalancedHost> wsHosts = new ArrayList<>();

    public void setCheckServerHealthInterval(long checkServerHealthInterval) {
        this.checkServerHealthInterval = checkServerHealthInterval;
    }

    public long getCheckServerHealthInterval() {
        return checkServerHealthInterval;
    }

    public int getServerFailuresBeforeDown() {
        return serverFailuresBeforeDown;
    }

    public void setServerFailuresBeforeDown(int serverFailuresBeforeDown) {
        this.serverFailuresBeforeDown = serverFailuresBeforeDown;
    }

    public int getServerSuccessesBeforeUp() {
        return serverSuccessesBeforeUp;
    }

    public void setServerSuccessesBeforeUp(int serverSuccessesBeforeUp) {
        this.serverSuccessesBeforeUp = serverSuccessesBeforeUp;
    }

    public GenericKeyedObjectPoolConfig getUdpClientPool() {
        return udpClientPool;
    }

    public GenericKeyedObjectPoolConfig getTcpClientPool() {
        return tcpClientPool;
    }

    public GenericKeyedObjectPoolConfig getHttpClientPool() {
        return httpClientPool;
    }

    public GenericKeyedObjectPoolConfig getWsClientPool() {
        return wsClientPool;
    }

    public ClientSsl getClientSsl() {
        return clientSsl;
    }

    public void setLoginRequired(boolean loginRequired) {
        this.loginRequired = loginRequired;
    }

    public boolean isLoginRequired() {
        return loginRequired;
    }

    public List<TimelyBalancedHost> getTimelyHosts() {
        return timelyHosts;
    }

    public class ClientSsl {

        private String keyFile;
        private String keyType;
        private String keyPassword;
        private String trustStoreFile;
        private String trustStoreType;
        private String trustStorePassword;

        public String getKeyFile() {
            return keyFile;
        }

        public BalancerConfiguration setKeyFile(String keyFile) {
            this.keyFile = keyFile;
            return BalancerConfiguration.this;
        }

        public String getKeyType() {
            return keyType;
        }

        public BalancerConfiguration setKeyType(String keyType) {
            this.keyType = keyType;
            return BalancerConfiguration.this;
        }

        public String getKeyPassword() {
            return keyPassword;
        }

        public BalancerConfiguration setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
            return BalancerConfiguration.this;
        }

        public String getTrustStoreFile() {
            return trustStoreFile;
        }

        public BalancerConfiguration setTrustStoreFile(String trustStoreFile) {
            this.trustStoreFile = trustStoreFile;
            return BalancerConfiguration.this;
        }

        public String getTrustStoreType() {
            return trustStoreType;
        }

        public BalancerConfiguration setTrustStoreType(String trustStoreType) {
            this.trustStoreType = trustStoreType;
            return BalancerConfiguration.this;
        }

        public String getTrustStorePassword() {
            return trustStorePassword;
        }

        public BalancerConfiguration setTrustStorePassword(String trustStorePassword) {
            this.trustStorePassword = trustStorePassword;
            return BalancerConfiguration.this;

        }
    }
}

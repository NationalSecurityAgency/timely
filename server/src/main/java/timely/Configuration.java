package timely;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.hibernate.validator.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

import timely.validator.NotEmptyIfFieldSet;

import com.google.common.collect.Lists;

@Component
@ConfigurationProperties(prefix = "timely")
public class Configuration {

    private String metricsTable = "timely.metrics";
    private String metaTable = "timely.meta";
    private HashMap<String, Integer> metricAgeOffDays = new HashMap<>();
    private List<String> metricsReportIgnoredTags = new ArrayList<>();

    @Valid
    @NestedConfigurationProperty
    private Accumulo accumulo = new Accumulo();
    @Valid
    @NestedConfigurationProperty
    private Security security = new Security();
    @Valid
    @NestedConfigurationProperty
    private Server server = new Server();
    @Valid
    @NestedConfigurationProperty
    private Http http = new Http();
    @Valid
    @NestedConfigurationProperty
    private MetaCache metaCache = new MetaCache();
    @Valid
    @NestedConfigurationProperty
    private VisibilityCache visibilityCache = new VisibilityCache();
    @Valid
    @NestedConfigurationProperty
    private Websocket websocket = new Websocket();

    public String getMetricsTable() {
        return metricsTable;
    }

    public Configuration setMetricsTable(String metricsTable) {
        this.metricsTable = metricsTable;
        return this;
    }

    public String getMetaTable() {
        return metaTable;
    }

    public Configuration setMetaTable(String metaTable) {
        this.metaTable = metaTable;
        return this;
    }

    public HashMap<String, Integer> getMetricAgeOffDays() {
        return metricAgeOffDays;
    }

    public void setMetricAgeOffDays(HashMap<String, Integer> metricAgeOffDays) {
        this.metricAgeOffDays = metricAgeOffDays;
    }

    public List<String> getMetricsReportIgnoredTags() {
        return metricsReportIgnoredTags;
    }

    public Configuration setMetricsReportIgnoredTags(List<String> metricsReportIgnoredTags) {
        this.metricsReportIgnoredTags = metricsReportIgnoredTags;
        return this;
    }

    public Accumulo getAccumulo() {
        return accumulo;
    }

    public Security getSecurity() {
        return security;
    }

    public Server getServer() {
        return server;
    }

    public Http getHttp() {
        return http;
    }

    public Websocket getWebsocket() {
        return websocket;
    }

    public MetaCache getMetaCache() {
        return metaCache;
    }

    public VisibilityCache getVisibilityCache() {
        return visibilityCache;
    }

    public class Accumulo {

        @NotBlank
        private String zookeepers;
        @NotBlank
        private String instanceName;
        @NotBlank
        private String username;
        @NotBlank
        private String password;
        @Valid
        @NestedConfigurationProperty
        private Write write = new Write();
        @Valid
        @NestedConfigurationProperty
        private Scan scan = new Scan();

        public String getInstanceName() {
            return instanceName;
        }

        public String getZookeepers() {
            return zookeepers;
        }

        public Configuration setZookeepers(String zookeepers) {
            this.zookeepers = zookeepers;
            return Configuration.this;
        }

        public Write getWrite() {
            return write;
        }

        public Scan getScan() {
            return scan;
        }

        public Configuration setInstanceName(String instanceName) {
            this.instanceName = instanceName;
            return Configuration.this;
        }

        public String getUsername() {
            return username;
        }

        public Configuration setUsername(String username) {
            this.username = username;
            return Configuration.this;
        }

        public String getPassword() {
            return password;
        }

        public Configuration setPassword(String password) {
            this.password = password;
            return Configuration.this;
        }
    }

    public class Write {

        private String latency = "5s";
        private int threads;
        private String bufferSize;

        public Write() {
            BatchWriterConfig config = new BatchWriterConfig();
            threads = config.getMaxWriteThreads();
            bufferSize = Long.toString(config.getMaxMemory());
        }

        public String getLatency() {
            return latency;
        }

        public Configuration setLatency(String latency) {
            this.latency = latency;
            return Configuration.this;
        }

        public int getThreads() {
            return threads;
        }

        public Configuration setThreads(int threads) {
            this.threads = threads;
            return Configuration.this;
        }

        public String getBufferSize() {
            return bufferSize;
        }

        public Configuration setBufferSize(String bufferSize) {
            this.bufferSize = bufferSize;
            return Configuration.this;
        }
    }

    public class Scan {

        private int threads = 4;

        public int getThreads() {
            return threads;
        }

        public Configuration setThreads(int threads) {
            this.threads = threads;
            return Configuration.this;
        }
    }

    public class Security {

        private boolean allowAnonymousAccess = false;
        private int sessionMaxAge = 86400;
        @Valid
        @NestedConfigurationProperty
        private Ssl ssl = new Ssl();

        public boolean isAllowAnonymousAccess() {
            return allowAnonymousAccess;
        }

        public Configuration setAllowAnonymousAccess(boolean allowAnonymousAccess) {
            this.allowAnonymousAccess = allowAnonymousAccess;
            return Configuration.this;
        }

        public int getSessionMaxAge() {
            return sessionMaxAge;
        }

        public Configuration setSessionMaxAge(int sessionMaxAge) {
            this.sessionMaxAge = sessionMaxAge;
            return Configuration.this;
        }

        public Ssl getSsl() {
            return ssl;
        }
    }

    @NotEmptyIfFieldSet.List({
            @NotEmptyIfFieldSet(fieldName = "useGeneratedKeypair", fieldValue = "false", notNullFieldName = "certificateFile", message = "must be set if timely.security.ssl.use-generated-keypair is false"),
            @NotEmptyIfFieldSet(fieldName = "useGeneratedKeypair", fieldValue = "false", notNullFieldName = "keyFile", message = "must be set if timely.security.ssl.use-generated-keypair is false") })
    public class Ssl {

        private String certificateFile;
        private String keyFile;
        private String keyPassword;
        private String trustStoreFile;
        private boolean useGeneratedKeypair = false;
        private boolean useOpenssl = true;
        private List<String> useCiphers = Lists.newArrayList("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", "TLS_RSA_WITH_AES_128_GCM_SHA256",
                "TLS_RSA_WITH_AES_128_CBC_SHA", "SSL_RSA_WITH_3DES_EDE_CBC_SHA");

        public String getCertificateFile() {
            return certificateFile;
        }

        public Configuration setCertificateFile(String certificateFile) {
            this.certificateFile = certificateFile;
            return Configuration.this;
        }

        public String getKeyFile() {
            return keyFile;
        }

        public Configuration setKeyFile(String keyFile) {
            this.keyFile = keyFile;
            return Configuration.this;
        }

        public String getKeyPassword() {
            return keyPassword;
        }

        public Configuration setKeyPassword(String keyPassword) {
            this.keyPassword = keyPassword;
            return Configuration.this;
        }

        public String getTrustStoreFile() {
            return trustStoreFile;
        }

        public Configuration setTrustStoreFile(String trustStoreFile) {
            this.trustStoreFile = trustStoreFile;
            return Configuration.this;
        }

        public boolean isUseGeneratedKeypair() {
            return useGeneratedKeypair;
        }

        public Configuration setUseGeneratedKeypair(boolean useGeneratedKeypair) {
            this.useGeneratedKeypair = useGeneratedKeypair;
            return Configuration.this;
        }

        public boolean isUseOpenssl() {
            return useOpenssl;
        }

        public Configuration setUseOpenssl(boolean useOpenssl) {
            this.useOpenssl = useOpenssl;
            return Configuration.this;
        }

        public List<String> getUseCiphers() {
            return useCiphers;
        }

        public Configuration setUseCiphers(List<String> useCiphers) {
            this.useCiphers = useCiphers;
            return Configuration.this;
        }
    }

    public class Server {

        @NotBlank
        private String ip;
        @NotNull
        private Integer tcpPort;
        @NotNull
        private Integer udpPort;

        @NotNull
        public String getIp() {
            return ip;
        }

        public Configuration setIp(String ip) {
            this.ip = ip;
            return Configuration.this;
        }

        public int getTcpPort() {
            return tcpPort;
        }

        public Configuration setTcpPort(int tcpPort) {
            this.tcpPort = tcpPort;
            return Configuration.this;
        }

        public int getUdpPort() {
            return udpPort;
        }

        public Configuration setUdpPort(Integer udpPort) {
            this.udpPort = udpPort;
            return Configuration.this;
        }
    }

    public class Http {

        @NotBlank
        private String ip;
        @NotNull
        private Integer port;
        @NotNull
        private String host;
        private String redirectPath = "/secure-me";
        private long strictTransportMaxAge = 604800;
        @Valid
        @NestedConfigurationProperty
        private Cors cors = new Cors();

        public String getIp() {
            return ip;
        }

        public Configuration setIp(String ip) {
            this.ip = ip;
            return Configuration.this;
        }

        public int getPort() {
            return port;
        }

        public Configuration setPort(int port) {
            this.port = port;
            return Configuration.this;
        }

        public String getHost() {
            return host;
        }

        public Configuration setHost(String host) {
            this.host = host;
            return Configuration.this;
        }

        public String getRedirectPath() {
            return redirectPath;
        }

        public Configuration setRedirectPath(String redirectPath) {
            this.redirectPath = redirectPath;
            return Configuration.this;
        }

        public long getStrictTransportMaxAge() {
            return strictTransportMaxAge;
        }

        public Configuration setStrictTransportMaxAge(long strictTransportMaxAge) {
            this.strictTransportMaxAge = strictTransportMaxAge;
            return Configuration.this;
        }

        public Cors getCors() {
            return cors;
        }
    }

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

        public Configuration setAllowAnyOrigin(boolean allowAnyOrigin) {
            this.allowAnyOrigin = allowAnyOrigin;
            return Configuration.this;
        }

        public boolean isAllowNullOrigin() {
            return allowNullOrigin;
        }

        public Configuration setAllowNullOrigin(boolean allowNullOrigin) {
            this.allowNullOrigin = allowNullOrigin;
            return Configuration.this;
        }

        public Set<String> getAllowedOrigins() {
            return allowedOrigins;
        }

        public Configuration setAllowedOrigins(Set<String> allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
            return Configuration.this;
        }

        public List<String> getAllowedMethods() {
            return allowedMethods;
        }

        public Configuration setAllowedMethods(List<String> allowedMethods) {
            this.allowedMethods = allowedMethods;
            return Configuration.this;
        }

        public List<String> getAllowedHeaders() {
            return allowedHeaders;
        }

        public Configuration setAllowedHeaders(List<String> allowedHeaders) {
            this.allowedHeaders = allowedHeaders;
            return Configuration.this;
        }

        public boolean isAllowCredentials() {
            return allowCredentials;
        }

        public Configuration setAllowCredentials(boolean allowCredentials) {
            this.allowCredentials = allowCredentials;
            return Configuration.this;
        }
    }

    public class Websocket {

        @NotBlank
        private String ip;
        @NotNull
        private Integer port;
        public int timeout = 60;
        public int subscriptionLag = 120;

        public String getIp() {
            return ip;
        }

        public Configuration setIp(String ip) {
            this.ip = ip;
            return Configuration.this;
        }

        public int getPort() {
            return port;
        }

        public Configuration setPort(int port) {
            this.port = port;
            return Configuration.this;
        }

        public int getTimeout() {
            return timeout;
        }

        public Configuration setTimeout(int timeout) {
            this.timeout = timeout;
            return Configuration.this;
        }

        public int getSubscriptionLag() {
            return subscriptionLag;
        }

        public Configuration setSubscriptionLag(int subscriptionLag) {
            this.subscriptionLag = subscriptionLag;
            return Configuration.this;
        }
    }

    public class MetaCache {

        private long expirationMinutes = 60;
        private int initialCapacity = 2000;
        private long maxCapacity = 10000;

        public long getExpirationMinutes() {
            return expirationMinutes;
        }

        public Configuration setExpirationMinutes(long expirationMinutes) {
            this.expirationMinutes = expirationMinutes;
            return Configuration.this;
        }

        public int getInitialCapacity() {
            return initialCapacity;
        }

        public Configuration setInitialCapacity(int initialCapacity) {
            this.initialCapacity = initialCapacity;
            return Configuration.this;
        }

        public long getMaxCapacity() {
            return maxCapacity;
        }

        public Configuration setMaxCapacity(long maxCapacity) {
            this.maxCapacity = maxCapacity;
            return Configuration.this;
        }
    }

    public class VisibilityCache {

        private long expirationMinutes = 60;
        private int initialCapacity = 2000;
        private long maxCapacity = 10000;

        public long getExpirationMinutes() {
            return expirationMinutes;
        }

        public Configuration setExpirationMinutes(long expirationMinutes) {
            this.expirationMinutes = expirationMinutes;
            return Configuration.this;
        }

        public int getInitialCapacity() {
            return initialCapacity;
        }

        public Configuration setInitialCapacity(int initialCapacity) {
            this.initialCapacity = initialCapacity;
            return Configuration.this;
        }

        public long getMaxCapacity() {
            return maxCapacity;
        }

        public Configuration setMaxCapacity(long maxCapacity) {
            this.maxCapacity = maxCapacity;
            return Configuration.this;
        }
    }
}

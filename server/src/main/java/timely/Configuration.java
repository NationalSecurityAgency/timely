package timely;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.hibernate.validator.constraints.NotEmpty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;
import timely.validator.NotEmptyIfFieldSet;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Component
@ConfigurationProperties(prefix = "timely")
@NotEmptyIfFieldSet.List({
        @NotEmptyIfFieldSet(fieldName = "ssl.useGeneratedKeypair", fieldValue = "false", notNullFieldName = "ssl.certificateFile", message = "must be set if timely.ssl.use-generated-keypair is false"),
        @NotEmptyIfFieldSet(fieldName = "ssl.useGeneratedKeypair", fieldValue = "false", notNullFieldName = "ssl.keyFile", message = "must be set if timely.ssl.use-generated-keypair is false") })
public class Configuration {

    @NotEmpty
    private String ip;
    @NotEmpty
    private String zookeepers;
    @NotEmpty
    private String instanceName;
    @NotEmpty
    private String username;
    @NotEmpty
    private String password;

    private String table = "timely.metrics";
    private String meta = "timely.meta";
    private int metricAgeOffDays = 7;
    private List<String> metricsReportIgnoredTags = new ArrayList<>();
    private boolean allowAnonymousAccess = false;
    private int sessionMaxAge = 86400;

    @NestedConfigurationProperty
    private Port ports = new Port();
    @NestedConfigurationProperty
    private Http http = new Http();
    @NestedConfigurationProperty
    private Write write = new Write();
    @NestedConfigurationProperty
    private Scanner scanner = new Scanner();
    @NestedConfigurationProperty
    private Cors cors = new Cors();
    @NestedConfigurationProperty
    private MetaCache metaCache = new MetaCache();
    @NestedConfigurationProperty
    private VisibilityCache visibilityCache = new VisibilityCache();
    @NestedConfigurationProperty
    private WebSocket webSocket = new WebSocket();
    @NestedConfigurationProperty
    private Ssl ssl = new Ssl();

    public String getIp() {
        return ip;
    }

    public Configuration setIp(String ip) {
        this.ip = ip;
        return this;
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public Configuration setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
        return this;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public Configuration setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public Configuration setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public Configuration setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getTable() {
        return table;
    }

    public Configuration setTable(String table) {
        this.table = table;
        return this;
    }

    public String getMeta() {
        return meta;
    }

    public Configuration setMeta(String meta) {
        this.meta = meta;
        return this;
    }

    public int getMetricAgeOffDays() {
        return metricAgeOffDays;
    }

    public Configuration setMetricAgeOffDays(int metricAgeOffDays) {
        this.metricAgeOffDays = metricAgeOffDays;
        return this;
    }

    public List<String> getMetricsReportIgnoredTags() {
        return metricsReportIgnoredTags;
    }

    public Configuration setMetricsReportIgnoredTags(List<String> metricsReportIgnoredTags) {
        this.metricsReportIgnoredTags = metricsReportIgnoredTags;
        return this;
    }

    public boolean isAllowAnonymousAccess() {
        return allowAnonymousAccess;
    }

    public Configuration setAllowAnonymousAccess(boolean allowAnonymousAccess) {
        this.allowAnonymousAccess = allowAnonymousAccess;
        return this;
    }

    public int getSessionMaxAge() {
        return sessionMaxAge;
    }

    public Configuration setSessionMaxAge(int sessionMaxAge) {
        this.sessionMaxAge = sessionMaxAge;
        return this;
    }

    public VisibilityCache getVisibilityCache() {
        return visibilityCache;
    }

    public Port getPort() {
        return ports;
    }

    public Http getHttp() {
        return http;
    }

    public Write getWrite() {
        return write;
    }

    public Scanner getScanner() {
        return scanner;
    }

    public Cors getCors() {
        return cors;
    }

    public MetaCache getMetaCache() {
        return metaCache;
    }

    public WebSocket getWebSocket() {
        return webSocket;
    }

    public Ssl getSsl() {
        return ssl;
    }

    public class Port {

        @NotNull
        private Integer put;
        @NotNull
        private Integer query;
        @NotNull
        private Integer websocket;

        public int getPut() {
            return put;
        }

        public Configuration setPut(int put) {
            this.put = put;
            return Configuration.this;
        }

        public int getQuery() {
            return query;
        }

        public Configuration setQuery(int query) {
            this.query = query;
            return Configuration.this;
        }

        public int getWebsocket() {
            return websocket;
        }

        public Configuration setWebsocket(int websocket) {
            this.websocket = websocket;
            return Configuration.this;
        }
    }

    public class Http {

        @NotNull
        private String host;
        private String redirectPath = "/secure-me";
        private long strictTransportMaxAge = 604800;

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

    public class Scanner {

        private int threads = 4;

        public int getThreads() {
            return threads;
        }

        public Configuration setThreads(int threads) {
            this.threads = threads;
            return Configuration.this;
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

    public class WebSocket {

        public int timeout = 60;
        public int subscriptionLag = 120;

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
}

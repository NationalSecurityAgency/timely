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

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
    }

    public int getMetricAgeOffDays() {
        return metricAgeOffDays;
    }

    public void setMetricAgeOffDays(int metricAgeOffDays) {
        this.metricAgeOffDays = metricAgeOffDays;
    }

    public List<String> getMetricsReportIgnoredTags() {
        return metricsReportIgnoredTags;
    }

    public void setMetricsReportIgnoredTags(List<String> metricsReportIgnoredTags) {
        this.metricsReportIgnoredTags = metricsReportIgnoredTags;
    }

    public boolean isAllowAnonymousAccess() {
        return allowAnonymousAccess;
    }

    public void setAllowAnonymousAccess(boolean allowAnonymousAccess) {
        this.allowAnonymousAccess = allowAnonymousAccess;
    }

    public int getSessionMaxAge() {
        return sessionMaxAge;
    }

    public void setSessionMaxAge(int sessionMaxAge) {
        this.sessionMaxAge = sessionMaxAge;
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

    public static class Port {

        @NotNull
        private Integer put;
        @NotNull
        private Integer query;
        @NotNull
        private Integer websocket;

        public int getPut() {
            return put;
        }

        public void setPut(int put) {
            this.put = put;
        }

        public int getQuery() {
            return query;
        }

        public void setQuery(int query) {
            this.query = query;
        }

        public int getWebsocket() {
            return websocket;
        }

        public void setWebsocket(int websocket) {
            this.websocket = websocket;
        }
    }

    public static class Http {

        @NotNull
        private String host;
        private String redirectPath = "/secure-me";
        private long strictTransportMaxAge = 604800;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public String getRedirectPath() {
            return redirectPath;
        }

        public void setRedirectPath(String redirectPath) {
            this.redirectPath = redirectPath;
        }

        public long getStrictTransportMaxAge() {
            return strictTransportMaxAge;
        }

        public void setStrictTransportMaxAge(long strictTransportMaxAge) {
            this.strictTransportMaxAge = strictTransportMaxAge;
        }
    }

    public static class Write {

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

        public void setLatency(String latency) {
            this.latency = latency;
        }

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }

        public String getBufferSize() {
            return bufferSize;
        }

        public void setBufferSize(String bufferSize) {
            this.bufferSize = bufferSize;
        }
    }

    public static class Scanner {

        private int threads = 4;

        public int getThreads() {
            return threads;
        }

        public void setThreads(int threads) {
            this.threads = threads;
        }
    }

    public static class Cors {

        private boolean allowAnyOrigin = false;
        private boolean allowNullOrigin = false;
        private Set<String> allowedOrigins = new HashSet<>();
        private List<String> allowedMethods = Lists.newArrayList("DELETE", "GET", "HEAD", "OPTIONS", "PUT", "POST");
        private List<String> allowedHeaders = Lists.newArrayList("content-type");
        private boolean allowCredentials = true;

        public boolean isAllowAnyOrigin() {
            return allowAnyOrigin;
        }

        public void setAllowAnyOrigin(boolean allowAnyOrigin) {
            this.allowAnyOrigin = allowAnyOrigin;
        }

        public boolean isAllowNullOrigin() {
            return allowNullOrigin;
        }

        public void setAllowNullOrigin(boolean allowNullOrigin) {
            this.allowNullOrigin = allowNullOrigin;
        }

        public Set<String> getAllowedOrigins() {
            return allowedOrigins;
        }

        public void setAllowedOrigins(Set<String> allowedOrigins) {
            this.allowedOrigins = allowedOrigins;
        }

        public List<String> getAllowedMethods() {
            return allowedMethods;
        }

        public void setAllowedMethods(List<String> allowedMethods) {
            this.allowedMethods = allowedMethods;
        }

        public List<String> getAllowedHeaders() {
            return allowedHeaders;
        }

        public void setAllowedHeaders(List<String> allowedHeaders) {
            this.allowedHeaders = allowedHeaders;
        }

        public boolean isAllowCredentials() {
            return allowCredentials;
        }

        public void setAllowCredentials(boolean allowCredentials) {
            this.allowCredentials = allowCredentials;
        }
    }

    public static class MetaCache {

        private long expirationMinutes = 60;
        private int initialCapacity = 2000;
        private long maxCapacity = 10000;

        public long getExpirationMinutes() {
            return expirationMinutes;
        }

        public void setExpirationMinutes(long expirationMinutes) {
            this.expirationMinutes = expirationMinutes;
        }

        public int getInitialCapacity() {
            return initialCapacity;
        }

        public void setInitialCapacity(int initialCapacity) {
            this.initialCapacity = initialCapacity;
        }

        public long getMaxCapacity() {
            return maxCapacity;
        }

        public void setMaxCapacity(long maxCapacity) {
            this.maxCapacity = maxCapacity;
        }
    }

    public static class Ssl {

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
    }

    public static class VisibilityCache {

        private long expirationMinutes = 60;
        private int initialCapacity = 2000;
        private long maxCapacity = 10000;

        public long getExpirationMinutes() {
            return expirationMinutes;
        }

        public void setExpirationMinutes(long expirationMinutes) {
            this.expirationMinutes = expirationMinutes;
        }

        public int getInitialCapacity() {
            return initialCapacity;
        }

        public void setInitialCapacity(int initialCapacity) {
            this.initialCapacity = initialCapacity;
        }

        public long getMaxCapacity() {
            return maxCapacity;
        }

        public void setMaxCapacity(long maxCapacity) {
            this.maxCapacity = maxCapacity;
        }
    }

    public static class WebSocket {

        public int timeout = 60;
        public int subscriptionLag = 120;

        public int getTimeout() {
            return timeout;
        }

        public void setTimeout(int timeout) {
            this.timeout = timeout;
        }

        public int getSubscriptionLag() {
            return subscriptionLag;
        }

        public void setSubscriptionLag(int subscriptionLag) {
            this.subscriptionLag = subscriptionLag;
        }
    }
}

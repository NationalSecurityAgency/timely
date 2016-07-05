package timely;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Configuration {

    public static final String IP = "timely.ip";
    public static final String PUT_PORT = "timely.port.put";
    public static final String QUERY_PORT = "timely.port.query";
    public static final String ZOOKEEPERS = "timely.zookeepers";
    public static final String INSTANCE_NAME = "timely.instance_name";
    public static final String USERNAME = "timely.username";
    public static final String PASSWORD = "timely.password";
    public static final String METRICS_TABLE = "timely.table";
    private static final String METRICS_TABLE_DEFAULT = "timely.metrics";
    public static final String META_TABLE = "timely.meta";
    private static final String META_TABLE_DEFAULT = "timely.meta";
    public static final String METRICS_AGEOFF_DAYS = "timely.metric.age.off.days";
    private static final String METRICS_AGEOFF_DAYS_DEFAULT = "7";

    public static final String MAX_LATENCY = "timely.write.latency";
    private static final String MAX_LATENCY_DEFAULT = "5s";
    public static final String WRITE_THREADS = "timely.write.threads";
    public static final String WRITE_BUFFER_SIZE = "timely.write.buffer.size";
    public static final String SCANNER_THREADS = "timely.scanner.threads";

    public static final String CORS_ALLOW_ANY_ORIGIN = "timely.cors.allow.any.origin";
    private static final String CORS_ALLOW_ANY_ORIGIN_DEFAULT = "false";
    public static final String CORS_ALLOW_NULL_ORIGIN = "timely.cors.allow.null.origin";
    private static final String CORS_ALLOW_NULL_ORIGIN_DEFAULT = "false";
    public static final String CORS_ALLOWED_ORIGINS = "timely.cors.allowed.origins";
    private static final String CORS_ALLOWED_ORIGINS_DEFAULT = "";
    public static final String CORS_ALLOWED_METHODS = "timely.cors.allowed.methods";
    private static final String CORS_ALLOWED_METHODS_DEFAULT = "DELETE,GET,HEAD,OPTIONS,PUT,POST";
    public static final String CORS_ALLOWED_HEADERS = "timely.cors.allowed.headers";
    private static final String CORS_ALLOWED_HEADERS_DEFAULT = "content-type";
    public static final String CORS_ALLOW_CREDENTIALS = "timely.cors.allow.credentials";
    private static final String CORS_ALLOW_CREDENTIALS_DEFAULT = "true";

    public static final String METRICS_IGNORED_TAGS = "timely.metrics.report.tags.ignored";

    public static final String META_CACHE_EXPIRATION = "timely.meta.cache.expiration.minutes";
    public static final Long META_CACHE_EXPIRATION_DEFAULT = 60L;
    public static final String META_CACHE_INITIAL_CAPACITY = "timely.meta.cache.initial.capacity";
    public static final Integer META_CACHE_INITIAL_CAPACITY_DEFAULT = 2000;
    public static final String META_CACHE_MAX_CAPACITY = "timely.meta.cache.max.capacity";
    public static final Integer META_CACHE_MAX_CAPACITY_DEFAULT = 10000;

    public static final String VISIBILITY_CACHE_EXPIRATION = "timely.visibility.cache.expiration.minutes";
    public static final Long VISIBILITY_EXPIRATION_DEFAULT = 60L;
    public static final String VISIBILITY_CACHE_INITIAL_CAPACITY = "timely.visibility.cache.initial.capacity";
    public static final Integer VISIBILITY_CACHE_INITIAL_CAPACITY_DEFAULT = 2000;
    public static final String VISIBILITY_CACHE_MAX_CAPACITY = "timely.visibility.cache.max.capacity";
    public static final Integer VISIBILITY_CACHE_MAX_CAPACITY_DEFAULT = 10000;

    /** Security properties */
    public static final String SSL_CERTIFICATE_FILE = "timely.ssl.certificate.file";
    public static final String SSL_PRIVATE_KEY_FILE = "timely.ssl.key.file";
    public static final String SSL_PRIVATE_KEY_PASS = "timely.ssl.key.pass";
    public static final String SSL_USE_GENERATED_KEYPAIR = "timely.ssl.use.generated.keypair";
    private static final String SSL_USE_GENERATED_KEYPAIR_DEFAULT = "false";
    public static final String SSL_TRUST_STORE_FILE = "timely.ssl.trust.store.file";
    public static final String SSL_USE_OPENSSL = "timely.ssl.use.openssl";
    private static final String SSL_USE_OPENSSL_DEFAULT = "true";
    public static final String SSL_USE_CIPHERS = "timely.ssl.use.ciphers";
    private static final String SSL_USE_CIPHERS_DEFAULT = "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:"
            + "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:TLS_RSA_WITH_AES_128_GCM_SHA256:"
            + "TLS_RSA_WITH_AES_128_CBC_SHA:SSL_RSA_WITH_3DES_EDE_CBC_SHA";

    public static final String SESSION_MAX_AGE = "timely.session.max.age";
    public static final long SESSION_MAX_AGE_DEFAULT = 86400;
    public static final String TIMELY_HTTP_HOST = "timely.http.host";
    public static final String GRAFANA_HTTP_ADDRESS = "grafana.http.address";
    public static final String ALLOW_ANONYMOUS_ACCESS = "timely.allow.anonymous.access";
    private static final String ALLOW_ANONYMOUS_ACCESS_DEFAULT = "false";

    public static final String NON_SECURE_REDIRECT_PATH = "timely.http.redirect.path";
    private static final String NON_SECURE_REDIRECT_PATH_DEFAULT = "/secure-me";
    public static final String STRICT_TRANSPORT_MAX_AGE = "timely.hsts.max.age";
    private static final String STRICT_TRANSPORT_MAX_AGE_DEFAULT = "604800";

    private static final List<String> REQUIRED_PROPERTIES = new ArrayList<>();
    static {
        REQUIRED_PROPERTIES.add(IP);
        REQUIRED_PROPERTIES.add(PUT_PORT);
        REQUIRED_PROPERTIES.add(QUERY_PORT);
        REQUIRED_PROPERTIES.add(ZOOKEEPERS);
        REQUIRED_PROPERTIES.add(INSTANCE_NAME);
        REQUIRED_PROPERTIES.add(USERNAME);
        REQUIRED_PROPERTIES.add(PASSWORD);
        REQUIRED_PROPERTIES.add(TIMELY_HTTP_HOST);
        REQUIRED_PROPERTIES.add(GRAFANA_HTTP_ADDRESS);
    };

    private final Properties props = new Properties();

    public Configuration(Path path) throws IOException {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        try (FileSystem fs = FileSystem.get(hadoopConf)) {
            try (InputStream inputStream = fs.open(path)) {
                init(inputStream);
            }
        }

    }

    public Configuration(File conf) throws IOException {
        try (InputStream inputStream = new FileInputStream(conf)) {
            init(inputStream);
        }
    }

    private void init(InputStream configStream) throws IOException {
        // Add defaults
        props.setProperty(METRICS_TABLE, METRICS_TABLE_DEFAULT);
        props.setProperty(META_TABLE, META_TABLE_DEFAULT);
        props.setProperty(MAX_LATENCY, MAX_LATENCY_DEFAULT);
        props.setProperty(METRICS_AGEOFF_DAYS, METRICS_AGEOFF_DAYS_DEFAULT);
        final BatchWriterConfig defaults = new BatchWriterConfig();
        props.setProperty(WRITE_BUFFER_SIZE, "" + defaults.getMaxMemory());
        props.setProperty(WRITE_THREADS, "" + defaults.getMaxWriteThreads());
        props.setProperty(SCANNER_THREADS, "" + 4);
        props.setProperty(CORS_ALLOW_ANY_ORIGIN, CORS_ALLOW_ANY_ORIGIN_DEFAULT);
        props.setProperty(CORS_ALLOW_NULL_ORIGIN, CORS_ALLOW_NULL_ORIGIN_DEFAULT);
        props.setProperty(CORS_ALLOWED_ORIGINS, CORS_ALLOWED_ORIGINS_DEFAULT);
        props.setProperty(CORS_ALLOWED_METHODS, CORS_ALLOWED_METHODS_DEFAULT);
        props.setProperty(CORS_ALLOWED_HEADERS, CORS_ALLOWED_HEADERS_DEFAULT);
        props.setProperty(CORS_ALLOW_CREDENTIALS, CORS_ALLOW_CREDENTIALS_DEFAULT);
        props.setProperty(META_CACHE_EXPIRATION, "" + META_CACHE_EXPIRATION_DEFAULT);
        props.setProperty(META_CACHE_INITIAL_CAPACITY, "" + META_CACHE_INITIAL_CAPACITY_DEFAULT);
        props.setProperty(META_CACHE_MAX_CAPACITY, "" + META_CACHE_MAX_CAPACITY_DEFAULT);
        props.setProperty(SSL_USE_GENERATED_KEYPAIR, SSL_USE_GENERATED_KEYPAIR_DEFAULT);
        props.setProperty(SSL_USE_OPENSSL, SSL_USE_OPENSSL_DEFAULT);
        props.setProperty(SSL_USE_CIPHERS, SSL_USE_CIPHERS_DEFAULT);
        props.setProperty(ALLOW_ANONYMOUS_ACCESS, ALLOW_ANONYMOUS_ACCESS_DEFAULT);
        props.setProperty(SESSION_MAX_AGE, SESSION_MAX_AGE_DEFAULT + "");

        props.setProperty(VISIBILITY_CACHE_EXPIRATION, VISIBILITY_EXPIRATION_DEFAULT + "");
        props.setProperty(VISIBILITY_CACHE_INITIAL_CAPACITY, VISIBILITY_CACHE_INITIAL_CAPACITY_DEFAULT + "");
        props.setProperty(VISIBILITY_CACHE_MAX_CAPACITY, VISIBILITY_CACHE_MAX_CAPACITY_DEFAULT + "");

        props.setProperty(NON_SECURE_REDIRECT_PATH, NON_SECURE_REDIRECT_PATH_DEFAULT);
        props.setProperty(STRICT_TRANSPORT_MAX_AGE, STRICT_TRANSPORT_MAX_AGE_DEFAULT);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(configStream, StandardCharsets.UTF_8))) {
            props.load(reader);
            validate();
        }
    }

    protected void validate() {
        REQUIRED_PROPERTIES.forEach(p -> {
            if (!props.containsKey(p) || StringUtils.isEmpty(props.getProperty(p))) {
                throw new IllegalArgumentException("Required property " + p + " must be specified.");
            }
        });
        if (!getBoolean(Configuration.SSL_USE_GENERATED_KEYPAIR)) {
            if (StringUtils.isEmpty(get(Configuration.SSL_CERTIFICATE_FILE))) {
                throw new IllegalArgumentException(Configuration.SSL_CERTIFICATE_FILE + "must be specified.");
            }
            if (StringUtils.isEmpty(get(Configuration.SSL_PRIVATE_KEY_FILE))) {
                throw new IllegalArgumentException(Configuration.SSL_PRIVATE_KEY_FILE + "must be specified.");
            }
        }
    }

    public String get(String property) {
        return props.getProperty(property);
    }

    public boolean getBoolean(String property) {
        return Boolean.parseBoolean(get(property));
    }
}

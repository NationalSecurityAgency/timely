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
    public static final String META_TABLE = "timely.meta";
    public static final String METRICS_AGEOFF_DAYS = "timely.metric.age.off.days";

    public static final String MAX_LATENCY = "timely.write.latency";
    public static final String WRITE_THREADS = "timely.write.threads";
    public static final String WRITE_BUFFER_SIZE = "timely.write.buffer.size";
    public static final String SCANNER_THREADS = "timely.scanner.threads";

    public static final String CORS_ALLOW_ANY_ORIGIN = "timely.cors.allow.any.origin";
    public static final String CORS_ALLOW_NULL_ORIGIN = "timely.cors.allow.null.origin";
    public static final String CORS_ALLOWED_ORIGINS = "timely.cors.allowed.origins";
    public static final String CORS_ALLOWED_METHODS = "timely.cors.allowed.methods";
    public static final String CORS_ALLOWED_HEADERS = "timely.cors.allowed.headers";
    public static final String CORS_ALLOW_CREDENTIALS = "timely.cors.allow.credentials";

    public static final String METRICS_IGNORED_TAGS = "timely.metrics.report.tags.ignored";

    public static final String META_CACHE_EXPIRATION = "timely.meta.cache.expiration.minutes";
    public static final Long META_CACHE_EXPIRATION_DEFAULT = 60L;
    public static final String META_CACHE_INITIAL_CAPACITY = "timely.meta.cache.initial.capacity";
    public static final Integer META_CACHE_INITIAL_CAPACITY_DEFAULT = 2000;
    public static final String META_CACHE_MAX_CAPACITY = "timely.meta.cache.max.capacity";
    public static final Integer META_CACHE_MAX_CAPACITY_DEFAULT = 10000;

    private static final List<String> REQUIRED_PROPERTIES = new ArrayList<>();
    static {
        REQUIRED_PROPERTIES.add(IP);
        REQUIRED_PROPERTIES.add(PUT_PORT);
        REQUIRED_PROPERTIES.add(QUERY_PORT);
        REQUIRED_PROPERTIES.add(ZOOKEEPERS);
        REQUIRED_PROPERTIES.add(INSTANCE_NAME);
        REQUIRED_PROPERTIES.add(USERNAME);
        REQUIRED_PROPERTIES.add(PASSWORD);
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
        props.setProperty(METRICS_TABLE, "timely.metrics");
        props.setProperty(META_TABLE, "timely.meta");
        props.setProperty(MAX_LATENCY, "5s");
        props.setProperty(METRICS_AGEOFF_DAYS, "7");
        final BatchWriterConfig defaults = new BatchWriterConfig();
        props.setProperty(WRITE_BUFFER_SIZE, "" + defaults.getMaxMemory());
        props.setProperty(WRITE_THREADS, "" + defaults.getMaxWriteThreads());
        props.setProperty(SCANNER_THREADS, "" + 4);
        props.setProperty(CORS_ALLOW_ANY_ORIGIN, "true");
        props.setProperty(CORS_ALLOW_NULL_ORIGIN, "false");
        props.setProperty(CORS_ALLOWED_ORIGINS, "");
        props.setProperty(CORS_ALLOWED_METHODS, "DELETE,GET,HEAD,OPTIONS,PUT,POST");
        props.setProperty(CORS_ALLOWED_HEADERS, "content-type");
        props.setProperty(CORS_ALLOW_CREDENTIALS, "true");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(configStream, StandardCharsets.UTF_8))) {
            props.load(reader);
            validate();
        }
    }

    protected void validate() {
        REQUIRED_PROPERTIES.forEach(p -> {
            if (!props.containsKey(p) || StringUtils.isEmpty(props.getProperty(p))) {
                throw new RuntimeException("Required property " + p + " must be specified.");
            }
        });
    }

    public String get(String property) {
        return props.getProperty(property);
    }
}

package timely.test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import timely.Configuration;

import com.google.common.io.Files;

public class TestConfiguration extends Properties {

    private static final long serialVersionUID = 1L;
    public static final String GRAFANA_HTTP_ADDRESS_DEFAULT = "http://localhost:3000/";
    public static final String TIMELY_HTTP_ADDRESS_DEFAULT = "localhost";

    public Configuration toConfiguration(File propertiesFile) throws Exception {
        this.store(Files.newWriter(propertiesFile, StandardCharsets.UTF_8), null);
        return new Configuration(propertiesFile);
    }

    public static TestConfiguration createMinimalConfigurationForTest() {
        TestConfiguration cfg = new TestConfiguration();
        cfg.put(Configuration.IP, "127.0.0.1");
        cfg.put(Configuration.PUT_PORT, "54321");
        cfg.put(Configuration.QUERY_PORT, "54322");
        cfg.put(Configuration.ZOOKEEPERS, "localhost:2181");
        cfg.put(Configuration.INSTANCE_NAME, "test");
        cfg.put(Configuration.USERNAME, "root");
        cfg.put(Configuration.PASSWORD, "secret");
        cfg.put(Configuration.TIMELY_HTTP_HOST, "localhost");
        cfg.put(Configuration.GRAFANA_HTTP_ADDRESS, GRAFANA_HTTP_ADDRESS_DEFAULT);
        cfg.put(Configuration.SSL_USE_GENERATED_KEYPAIR, "true");
        return cfg;
    }

}

package timely;

import java.io.File;
import java.io.FileWriter;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConfigurationTest {

    @ClassRule
    public static final TemporaryFolder temp = new TemporaryFolder();

    @BeforeClass
    public static void before() throws Exception {
        temp.create();
    }

    @Test
    public void testMinimalConfiguration() throws Exception {
        File conf = temp.newFile("config.properties");
        conf.deleteOnExit();
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write(Configuration.IP + "=127.0.0.1\n");
            writer.write(Configuration.PUT_PORT + "=54321\n");
            writer.write(Configuration.QUERY_PORT + "=54322\n");
            writer.write(Configuration.ZOOKEEPERS + "=localhost:2181\n");
            writer.write(Configuration.INSTANCE_NAME + "=test\n");
            writer.write(Configuration.USERNAME + "=root\n");
            writer.write(Configuration.PASSWORD + "=secret\n");
            writer.write(Configuration.TIMELY_HTTP_ADDRESS + "=localhost\n");
            writer.write(Configuration.GRAFANA_HTTP_ADDRESS + "=http://localhost:3000/\n");
            writer.write(Configuration.SSL_USE_GENERATED_KEYPAIR + "=true\n");
        }
        new Configuration(conf);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingSSLProperty() throws Exception {
        File conf = temp.newFile("config2.properties");
        conf.deleteOnExit();
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write(Configuration.IP + "=127.0.0.1\n");
            writer.write(Configuration.PUT_PORT + "=54321\n");
            writer.write(Configuration.QUERY_PORT + "=54322\n");
            writer.write(Configuration.ZOOKEEPERS + "=localhost:2181\n");
            writer.write(Configuration.INSTANCE_NAME + "=test\n");
            writer.write(Configuration.USERNAME + "=root\n");
            writer.write(Configuration.PASSWORD + "=secret\n");
            writer.write(Configuration.TIMELY_HTTP_ADDRESS + "=localhost\n");
            writer.write(Configuration.GRAFANA_HTTP_ADDRESS + "=http://localhost:3000/\n");
        }
        new Configuration(conf);
    }

    @Test
    public void testSSLProperty() throws Exception {
        File conf = temp.newFile("config3.properties");
        conf.deleteOnExit();
        try (FileWriter writer = new FileWriter(conf)) {
            writer.write(Configuration.IP + "=127.0.0.1\n");
            writer.write(Configuration.PUT_PORT + "=54321\n");
            writer.write(Configuration.QUERY_PORT + "=54322\n");
            writer.write(Configuration.ZOOKEEPERS + "=localhost:2181\n");
            writer.write(Configuration.INSTANCE_NAME + "=test\n");
            writer.write(Configuration.USERNAME + "=root\n");
            writer.write(Configuration.PASSWORD + "=secret\n");
            writer.write(Configuration.TIMELY_HTTP_ADDRESS + "=localhost\n");
            writer.write(Configuration.GRAFANA_HTTP_ADDRESS + "=http://localhost:3000/\n");
            writer.write(Configuration.SSL_CERTIFICATE_FILE + "=/tmp/foo\n");
            writer.write(Configuration.SSL_PRIVATE_KEY_FILE + "=/tmp/bar\n");
        }
        new Configuration(conf);
    }

}

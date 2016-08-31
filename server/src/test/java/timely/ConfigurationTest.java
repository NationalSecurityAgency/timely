package timely;

import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigurationTest {

    private final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    @After
    public void closeContext() {
        context.close();
    }

    @Test
    public void testMinimalConfiguration() throws Exception {
        context.register(SpringBootstrap.class);
        // @formatter:off
        EnvironmentTestUtils.addEnvironment(this.context,
                "timely.server.ip:127.0.0.1",
                "timely.server.tcp-port:54321",
                "timely.server.udp-port:54325",
                "timely.http.ip:127.0.0.1",
                "timely.http.port:54322",
                "timely.websocket.ip:127.0.0.1",
                "timely.websocket.port:54323",
                "timely.accumulo.zookeepers:localhost:2181",
                "timely.accumulo.instance-name:test",
                "timely.accumulo.username:root",
                "timely.accumulo.password:secret",
                "timely.http.host:localhost",
                "timely.security.ssl.use-generated-keypair:true",
                "timely.metric-age-off-days[default]:7");
        // @formatter:on
        context.refresh();
        Configuration config = this.context.getBean(Configuration.class);
        assertEquals("127.0.0.1", config.getServer().getIp());
        assertEquals(54321, config.getServer().getTcpPort());
        assertEquals(54325, config.getServer().getUdpPort());
        assertEquals(54322, config.getHttp().getPort());
        assertEquals(54323, config.getWebsocket().getPort());
        assertEquals("localhost:2181", config.getAccumulo().getZookeepers());
        assertEquals("test", config.getAccumulo().getInstanceName());
        assertEquals("root", config.getAccumulo().getUsername());
        assertEquals("secret", config.getAccumulo().getPassword());
        assertEquals("localhost", config.getHttp().getHost());
        assertTrue(config.getSecurity().getSsl().isUseGeneratedKeypair());
        assertEquals(1, config.getMetricAgeOffDays().size());
        assertTrue(config.getMetricAgeOffDays().containsKey("default"));
        assertTrue(7 == config.getMetricAgeOffDays().get("default"));
    }

    @Test(expected = BeanCreationException.class)
    public void testMissingSSLProperty() throws Exception {
        context.register(SpringBootstrap.class);
        // @formatter:off
        EnvironmentTestUtils.addEnvironment(this.context,
                "timely.server.ip:127.0.0.1",
                "timely.server.tcp-port:54321",
                "timely.server.udp-port:54325",
                "timely.http.ip:127.0.0.1",
                "timely.http.port:54322",
                "timely.websocket.ip:127.0.0.1",
                "timely.websocket.port:54323",
                "timely.accumulo.zookeepers:localhost:2181",
                "timely.accumulo.instance-name:test",
                "timely.accumulo.username:root",
                "timely.accumulo.password:secret",
                "timely.http.host:localhost");
        // @formatter:on
        context.refresh();
    }

    @Test
    public void testSSLProperty() throws Exception {
        context.register(SpringBootstrap.class);
        // @formatter:off
        EnvironmentTestUtils.addEnvironment(this.context,
                "timely.server.ip:127.0.0.1",
                "timely.server.tcp-port:54321",
                "timely.server.udp-port:54325",
                "timely.http.ip:127.0.0.1",
                "timely.http.port:54322",
                "timely.websocket.ip:127.0.0.1",
                "timely.websocket.port:54323",
                "timely.accumulo.zookeepers:localhost:2181",
                "timely.accumulo.instance-name:test",
                "timely.accumulo.username:root",
                "timely.accumulo.password:secret",
                "timely.http.host:localhost",
                "timely.security.ssl.certificate-file:/tmp/foo",
                "timely.security.ssl.key-file:/tmp/bar");
        // @formatter:on
        context.refresh();
    }

}

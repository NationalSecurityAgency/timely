package timely;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import timely.configuration.Configuration;
import timely.configuration.SpringBootstrap;

public class ConfigurationTest {

    private AnnotationConfigApplicationContext context;

    @Before
    public void createContext() {
        context = new AnnotationConfigApplicationContext();
    }

    @After
    public void closeContext() {
        context.close();
    }

    @Test
    public void testMinimalConfiguration() throws Exception {
        context.register(SpringBootstrap.class);
        // @formatter:off
        TestPropertyValues.of(
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
                "timely.security.serverSsl.use-generated-keypair:true",
                "timely.metric-age-off-days[default]:7").applyTo(this.context);
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
        assertTrue(config.getSecurity().getServerSsl().isUseGeneratedKeypair());
        assertEquals(1, config.getMetricAgeOffDays().size());
        assertTrue(config.getMetricAgeOffDays().containsKey("default"));
        assertTrue(7 == config.getMetricAgeOffDays().get("default"));
    }

    @Test(expected = BeanCreationException.class)
    public void testMissingSSLProperty() throws Exception {
        context.register(SpringBootstrap.class);
        // @formatter:off
        TestPropertyValues.of(
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
                "timely.http.host:localhost").applyTo(this.context);
        // @formatter:on
        context.refresh();
    }

    @Test
    public void testSSLProperty() throws Exception {
        context.register(SpringBootstrap.class);
        // @formatter:off
        TestPropertyValues.of(
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
                "timely.security.serverSsl.key-store-file:/tmp/foo",
                "timely.security.serverSsl.key-store-password:password").applyTo(this.context);
        // @formatter:on
        context.refresh();
    }

}

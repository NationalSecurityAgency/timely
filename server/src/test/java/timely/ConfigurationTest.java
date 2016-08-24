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
        EnvironmentTestUtils.addEnvironment(this.context, "timely.ip:127.0.0.1", "timely.port.put:54321",
                "timely.port.query:54322", "timely.port.websocket:54323", "timely.zookeepers:localhost:2181",
                "timely.instance-name:test", "timely.username:root", "timely.password:secret",
                "timely.http.host:localhost", "timely.ssl.use-generated-keypair:true");
        context.refresh();
        Configuration config = this.context.getBean(Configuration.class);
        assertEquals("127.0.0.1", config.getIp());
        assertEquals(54321, config.getPort().getPut());
        assertEquals(54322, config.getPort().getQuery());
        assertEquals(54323, config.getPort().getWebsocket());
        assertEquals("localhost:2181", config.getZookeepers());
        assertEquals("test", config.getInstanceName());
        assertEquals("root", config.getUsername());
        assertEquals("secret", config.getPassword());
        assertEquals("localhost", config.getHttp().getHost());
        assertTrue(config.getSsl().isUseGeneratedKeypair());
    }

    @Test(expected = BeanCreationException.class)
    public void testMissingSSLProperty() throws Exception {
        context.register(SpringBootstrap.class);
        EnvironmentTestUtils.addEnvironment(this.context, "timely.ip:127.0.0.1", "timely.port.put:54321",
                "timely.port.query:54322", "timely.port.websocket:54323", "timely.zookeepers:localhost:2181",
                "timely.instance-name:test", "timely.username:root", "timely.password:secret",
                "timely.http.host:localhost");
        context.refresh();
    }

    @Test
    public void testSSLProperty() throws Exception {
        context.register(SpringBootstrap.class);
        EnvironmentTestUtils.addEnvironment(this.context, "timely.ip:127.0.0.1", "timely.port.put:54321",
                "timely.port.query:54322", "timely.port.websocket:54323", "timely.zookeepers:localhost:2181",
                "timely.instance-name:test", "timely.username:root", "timely.password:secret",
                "timely.http.host:localhost", "timely.ssl.certificate-file:/tmp/foo", "timely.ssl.key-file:/tmp/bar");
        context.refresh();
    }

}

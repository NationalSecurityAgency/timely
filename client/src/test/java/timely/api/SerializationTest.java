package timely.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import timely.serialize.JsonSerializer;

public class SerializationTest {

    private ObjectMapper mapper = JsonSerializer.getObjectMapper();

    @Test
    public void testBasicAuth() throws Exception {
        BasicAuthLogin login = new BasicAuthLogin();
        login.setUsername("test");
        login.setPassword("pass");
        testSerialization(login);
    }

    @Test
    public void testCreateSubscription() throws Exception {
        CreateSubscription create = new CreateSubscription();
        create.setSubscriptionId("1234");
        testSerialization(create);
    }

    @Test
    public void testCloseSubscription() throws Exception {
        CloseSubscription close = new CloseSubscription();
        close.setSubscriptionId("1234");
        testSerialization(close);
    }

    @Test
    public void testAddSubscription() throws Exception {
        AddSubscription add = new AddSubscription();
        add.setSubscriptionId("1234");
        add.setMetric("sys.cpu.user");
        testSerialization(add);
    }

    @Test
    public void testRemoveSubscription() throws Exception {
        RemoveSubscription remove = new RemoveSubscription();
        remove.setSubscriptionId("1234");
        remove.setMetric("sys.cpu.user");
        testSerialization(remove);
    }

    @SuppressWarnings("unchecked")
    private <T> void testSerialization(T o) throws Exception {
        String ser = mapper.writeValueAsString(o);
        T des = mapper.readValue(ser, (Class<T>) o.getClass());
        Assert.assertEquals(o, des);
    }
}

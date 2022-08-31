package timely.api;

import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import timely.serialize.JsonSerializer;

public class SerializationTest {

    private ObjectMapper mapper = JsonSerializer.getObjectMapper();

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

    private <T> void testSerialization(T o) throws Exception {
        String ser = mapper.writeValueAsString(o);
        @SuppressWarnings("unchecked")
        T des = mapper.readValue(ser, (Class<T>) o.getClass());
        Assert.assertEquals(o, des);
    }
}

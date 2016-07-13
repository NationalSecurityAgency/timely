package timely.api.request.subscription;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import timely.api.request.WebSocketRequest;
import timely.api.request.subscription.AddSubscription;
import timely.api.request.subscription.CloseSubscription;
import timely.api.request.subscription.CreateSubscription;
import timely.api.request.subscription.RemoveSubscription;
import timely.util.JsonUtil;

public class WebSocketRequestDeserializationTest {

    @Test
    public void testCreateDeserialization() throws Exception {
        // @formatter:off
		String json = "{ "
				       + "\"operation\" : \"create\","
				       + " \"sessionId\": \"1234\""
				    + "}";
		// @formatter:on
        WebSocketRequest request = JsonUtil.getObjectMapper().readValue(json.getBytes(), WebSocketRequest.class);
        Assert.assertNotNull(request);
        Assert.assertEquals(CreateSubscription.class, request.getClass());
        Assert.assertEquals("1234", ((CreateSubscription) request).getSessionId());
    }

    @Test
    public void testRemoveDeserialization() throws Exception {
        // @formatter:off
		String json = "{ "
				       + "\"operation\" : \"remove\","
				       + " \"sessionId\": \"1234\","
				       + " \"metric\" : \"sys.cpu.user\""
				    + "}";
		// @formatter:on
        WebSocketRequest request = JsonUtil.getObjectMapper().readValue(json.getBytes(), WebSocketRequest.class);
        Assert.assertNotNull(request);
        Assert.assertEquals(RemoveSubscription.class, request.getClass());
        Assert.assertEquals("1234", ((RemoveSubscription) request).getSessionId());
        Assert.assertEquals("sys.cpu.user", ((RemoveSubscription) request).getMetric());
    }

    @Test
    public void testCloseDeserialization() throws Exception {
        // @formatter:off
		String json = "{ "
				       + "\"operation\" : \"close\","
				       + " \"sessionId\": \"1234\""
				    + "}";
		// @formatter:on
        WebSocketRequest request = JsonUtil.getObjectMapper().readValue(json.getBytes(), WebSocketRequest.class);
        Assert.assertNotNull(request);
        Assert.assertEquals(CloseSubscription.class, request.getClass());
        Assert.assertEquals("1234", ((CloseSubscription) request).getSessionId());
    }

    @Test
    public void testAddDeserialization() throws Exception {
        // @formatter:off
		String json = "{" +
						"\"operation\" : \"add\"," +
						"\"sessionId\" : \"1234\"," +
					    " \"metric\" : \"sys.cpu.user\"" +
					  "}";
		// @formatter:on
        WebSocketRequest request = JsonUtil.getObjectMapper().readValue(json.getBytes(), WebSocketRequest.class);
        Assert.assertNotNull(request);
        Assert.assertEquals(AddSubscription.class, request.getClass());
        Assert.assertEquals("1234", ((AddSubscription) request).getSessionId());
        Assert.assertEquals("sys.cpu.user", ((AddSubscription) request).getMetric());
        Assert.assertEquals(false, ((AddSubscription) request).getTags().isPresent());
        Assert.assertEquals(false, ((AddSubscription) request).getStartTime().isPresent());
    }

    @Test
    public void testAddDeserializationWithTime() throws Exception {
        // @formatter:off
		String json = "{" +
						"\"operation\" : \"add\"," +
						"\"sessionId\" : \"1234\"," +
					    "\"metric\" : \"sys.cpu.user\"," +
						"\"startTime\" : \"1000\"" +
					  "}";
		// @formatter:on
        WebSocketRequest request = JsonUtil.getObjectMapper().readValue(json.getBytes(), WebSocketRequest.class);
        Assert.assertNotNull(request);
        Assert.assertEquals(AddSubscription.class, request.getClass());
        Assert.assertEquals("1234", ((AddSubscription) request).getSessionId());
        Assert.assertEquals("sys.cpu.user", ((AddSubscription) request).getMetric());
        Assert.assertEquals(false, ((AddSubscription) request).getTags().isPresent());
        Assert.assertEquals(true, ((AddSubscription) request).getStartTime().isPresent());
        long time = ((AddSubscription) request).getStartTime().get();
        Assert.assertEquals(1000L, time);
    }

    @Test
    public void testAddDeserializationWithTimeAndTags() throws Exception {
        // @formatter:off
		String json = "{" +
						"\"operation\" : \"add\"," +
						"\"sessionId\" : \"1234\"," +
					    "\"metric\" : \"sys.cpu.user\"," +
						"\"tags\" : {" +
					       "\"tag2\" : \"value2\"," +
					       "\"tag1\" : \"value1\"" +
					    "}," +
						"\"startTime\" : \"1000\"" +
					  "}";
		// @formatter:on
        WebSocketRequest request = JsonUtil.getObjectMapper().readValue(json.getBytes(), WebSocketRequest.class);
        Assert.assertNotNull(request);
        Assert.assertEquals(AddSubscription.class, request.getClass());
        Assert.assertEquals("1234", ((AddSubscription) request).getSessionId());
        Assert.assertEquals("sys.cpu.user", ((AddSubscription) request).getMetric());
        Assert.assertEquals(true, ((AddSubscription) request).getTags().isPresent());
        Map<String, String> tags = ((AddSubscription) request).getTags().get();
        Assert.assertTrue(tags.containsKey("tag1"));
        Assert.assertEquals("value1", tags.get("tag1"));
        Assert.assertTrue(tags.containsKey("tag2"));
        Assert.assertEquals("value2", tags.get("tag2"));
        Assert.assertEquals(true, ((AddSubscription) request).getStartTime().isPresent());
        long time = ((AddSubscription) request).getStartTime().get();
        Assert.assertEquals(1000L, time);
    }

    @Test
    public void testAddDeserializationWithStartAndDelayTimeAndTags() throws Exception {
        // @formatter:off
		String json = "{" +
						"\"operation\" : \"add\"," +
						"\"sessionId\" : \"1234\"," +
					    "\"metric\" : \"sys.cpu.user\"," +
						"\"tags\" : {" +
					       "\"tag2\" : \"value2\"," +
					       "\"tag1\" : \"value1\"" +
					    "}," +
						"\"startTime\" : \"1000\"," +
					    "\"delayTime\" : \"500\"" +
					  "}";
		// @formatter:on
        WebSocketRequest request = JsonUtil.getObjectMapper().readValue(json.getBytes(), WebSocketRequest.class);
        Assert.assertNotNull(request);
        Assert.assertEquals(AddSubscription.class, request.getClass());
        Assert.assertEquals("1234", ((AddSubscription) request).getSessionId());
        Assert.assertEquals("sys.cpu.user", ((AddSubscription) request).getMetric());
        Assert.assertEquals(true, ((AddSubscription) request).getTags().isPresent());
        Map<String, String> tags = ((AddSubscription) request).getTags().get();
        Assert.assertTrue(tags.containsKey("tag1"));
        Assert.assertEquals("value1", tags.get("tag1"));
        Assert.assertTrue(tags.containsKey("tag2"));
        Assert.assertEquals("value2", tags.get("tag2"));
        Assert.assertEquals(true, ((AddSubscription) request).getStartTime().isPresent());
        long time = ((AddSubscription) request).getStartTime().get();
        Assert.assertEquals(1000L, time);
        long delay = ((AddSubscription) request).getDelayTime().get();
        Assert.assertEquals(500L, delay);
    }

}

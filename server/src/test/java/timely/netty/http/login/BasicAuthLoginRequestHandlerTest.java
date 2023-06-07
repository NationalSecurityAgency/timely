package timely.netty.http.login;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import timely.api.request.auth.BasicAuthLoginRequest;
import timely.auth.AuthCache;
import timely.configuration.Configuration;
import timely.netty.Constants;
import timely.netty.http.HttpRequestDecoder;
import timely.netty.http.auth.BasicAuthLoginRequestHandler;
import timely.test.CaptureChannelHandlerContext;
import timely.test.TestConfiguration;

public class BasicAuthLoginRequestHandlerTest {

    private static class TestHttpQueryDecoder extends HttpRequestDecoder {

        public TestHttpQueryDecoder(Configuration config) {
            super(config.getSecurity(), config.getHttp());
        }

        @Override
        public void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception {
            super.decode(ctx, msg, out);
        }

    }

    private List<Object> results = new ArrayList<>();

    @BeforeClass
    public static void before() throws Exception {
        AuthCache.configure(TestConfiguration.createMinimalConfigurationForTest().getSecurity());
    }

    @AfterClass
    public static void after() throws Exception {
        AuthCache.resetConfiguration();
    }

    @Before
    public void setup() throws Exception {
        AuthCache.clear();
        results.clear();
    }

    @Test
    public void testBasicAuthentication() throws Exception {
        Configuration config = TestConfiguration.createMinimalConfigurationForTest();

        // @formatter:off
        String form =
        "{\n" +
        "    \"username\": \"test\",\n" +
        "    \"password\": \"test1\"\n" +
        "}";
        // @formatter:on
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/login");
        request.content().writeBytes(form.getBytes());

        TestHttpQueryDecoder decoder = new TestHttpQueryDecoder(config);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Object result = results.iterator().next();
        Assert.assertEquals(BasicAuthLoginRequest.class, result.getClass());

        BasicAuthLoginRequestHandler handler = new BasicAuthLoginRequestHandler(config.getSecurity(), config.getHttp());
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        handler.channelRead(ctx, result);
        Assert.assertNotNull(ctx.msg);
        Assert.assertTrue(ctx.msg instanceof DefaultFullHttpResponse);
        DefaultFullHttpResponse response = (DefaultFullHttpResponse) ctx.msg;
        Assert.assertEquals(HttpResponseStatus.OK, response.status());
        Assert.assertTrue(response.headers().contains(HttpHeaderNames.CONTENT_TYPE));
        Assert.assertEquals(Constants.JSON_TYPE, response.headers().get(HttpHeaderNames.CONTENT_TYPE));
        Assert.assertTrue(response.headers().contains(HttpHeaderNames.SET_COOKIE));
        Cookie c = ClientCookieDecoder.STRICT.decode(response.headers().get(HttpHeaderNames.SET_COOKIE));
        Assert.assertEquals(TestConfiguration.TIMELY_HTTP_ADDRESS_DEFAULT, c.domain());
        Assert.assertEquals(86400, c.maxAge());
        Assert.assertTrue(c.isHttpOnly());
        Assert.assertTrue(c.isSecure());
        Assert.assertEquals(Constants.COOKIE_NAME, c.name());
        UUID.fromString(c.value());
    }

    @Test
    public void testBasicAuthenticationFailure() throws Exception {
        Configuration config = TestConfiguration.createMinimalConfigurationForTest();

        // @formatter:off
        String form =
        "{\n" +
        "    \"username\": \"test\",\n" +
        "    \"password\": \"test2\"\n" +
        "}";
        // @formatter:on
        DefaultFullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/login");
        request.content().writeBytes(form.getBytes());

        TestHttpQueryDecoder decoder = new TestHttpQueryDecoder(config);
        decoder.decode(null, request, results);
        Assert.assertEquals(1, results.size());
        Object result = results.iterator().next();
        Assert.assertEquals(BasicAuthLoginRequest.class, result.getClass());

        BasicAuthLoginRequestHandler handler = new BasicAuthLoginRequestHandler(config.getSecurity(), config.getHttp());
        CaptureChannelHandlerContext ctx = new CaptureChannelHandlerContext();
        handler.channelRead(ctx, result);
        Assert.assertNotNull(ctx.msg);
        Assert.assertTrue(ctx.msg instanceof DefaultFullHttpResponse);
        DefaultFullHttpResponse response = (DefaultFullHttpResponse) ctx.msg;
        Assert.assertEquals(HttpResponseStatus.UNAUTHORIZED, response.status());
        Assert.assertTrue(response.headers().contains(HttpHeaderNames.CONTENT_TYPE));
        Assert.assertEquals(Constants.JSON_TYPE, response.headers().get(HttpHeaderNames.CONTENT_TYPE));
    }

}

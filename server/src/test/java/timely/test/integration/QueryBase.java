package timely.test.integration;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.Assert.assertEquals;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.fasterxml.jackson.databind.JavaType;

import io.netty.handler.codec.http.HttpHeaderNames;
import timely.api.request.timeseries.QueryRequest;
import timely.api.response.timeseries.QueryResponse;
import timely.server.component.TestDataStore;
import timely.util.JsonUtil;

public abstract class QueryBase extends ITBase {

    public static class NotSuccessfulException extends Exception {

        private static final long serialVersionUID = 1L;
    }

    public static class UnauthorizedUserException extends Exception {

        private static final long serialVersionUID = 1L;

        public UnauthorizedUserException() {}
    }

    @Before
    public void setup() {
        super.setup();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    protected void put(String... lines) throws Exception {
        final CountDownLatch PUT_REQUESTS = new CountDownLatch(lines.length);
        TestDataStore.StoreCallback storeCallback = () -> PUT_REQUESTS.countDown();
        try {
            dataStore.addStoreCallback(storeCallback);
            StringBuffer format = new StringBuffer();
            for (String line : lines) {
                format.append("put ");
                format.append(line);
                format.append("\n");
            }
            try (Socket sock = new Socket(serverProperties.getIp(), serverProperties.getTcpPort());
                            PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
                writer.write(format.toString());
                writer.flush();
            }
            PUT_REQUESTS.await(5, TimeUnit.SECONDS);
        } finally {
            dataStore.removeStoreCallback(storeCallback);
        }
    }

    protected String query(String username, String password, String getRequest) throws Exception {
        URL url = new URL(getRequest);
        HttpsURLConnection con = getUrlConnection(username, password, url);
        int responseCode = con.getResponseCode();
        assertEquals(200, responseCode);
        String result = IOUtils.toString(con.getInputStream(), UTF_8);
        log.debug("Result is {}", result);
        return result;
    }

    protected String query(String getRequest) throws Exception {
        return query(getRequest, 200, null);
    }

    protected String query(String getRequest, String acceptType) throws Exception {
        return query(getRequest, 200, acceptType);
    }

    protected String query(String getRequest, int expectedResponseCode, String acceptType) throws Exception {
        URL url = new URL(getRequest);
        HttpsURLConnection con = getUrlConnection(url);
        if (null != acceptType) {
            log.trace("Setting Accept header to {}", acceptType);
            con.addRequestProperty(HttpHeaderNames.ACCEPT.toString(), acceptType);
        }
        log.trace("Sending HTTP Headers: {}", con.getRequestProperties());
        int responseCode = con.getResponseCode();
        assertEquals(expectedResponseCode, responseCode);
        if (200 == responseCode) {
            String result = IOUtils.toString(con.getInputStream(), UTF_8);
            log.debug("Result is {}", result);
            return result;
        } else {
            throw new NotSuccessfulException();
        }
    }

    protected List<QueryResponse> query(String username, String password, String location, QueryRequest request) throws Exception {
        return query(username, password, location, request, 200);
    }

    protected List<QueryResponse> query(String username, String password, String location, QueryRequest request, int expectedResponseCode) throws Exception {
        URL url = new URL(location);
        HttpsURLConnection con = getUrlConnection(username, password, url);
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        String requestJSON = JsonUtil.getObjectMapper().writeValueAsString(request);
        con.setRequestProperty("Content-Length", String.valueOf(requestJSON.length()));
        OutputStream wr = con.getOutputStream();
        wr.write(requestJSON.getBytes(UTF_8));
        int responseCode = con.getResponseCode();
        Assert.assertEquals(expectedResponseCode, responseCode);
        if (200 == responseCode) {
            String result = IOUtils.toString(con.getInputStream(), UTF_8);
            log.debug("Result is {}", result);
            JavaType type = JsonUtil.getObjectMapper().getTypeFactory().constructCollectionType(List.class, QueryResponse.class);
            return JsonUtil.getObjectMapper().readValue(result, type);
        } else {
            throw new NotSuccessfulException();
        }
    }

    protected List<QueryResponse> query(String location, QueryRequest request) throws Exception {
        return query(location, request, 200);
    }

    protected List<QueryResponse> query(String location, QueryRequest request, int expectedResponseCode) throws Exception {
        return query(getUrlConnection(new URL(location)), request, expectedResponseCode);
    }

    protected List<QueryResponse> query(HttpsURLConnection con, QueryRequest request, int expectedResponseCode) throws Exception {
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        String requestJSON = JsonUtil.getObjectMapper().writeValueAsString(request);
        con.setRequestProperty("Content-Length", String.valueOf(requestJSON.length()));
        log.debug("Request Body JSON: {}", requestJSON);
        OutputStream wr = con.getOutputStream();
        wr.write(requestJSON.getBytes(UTF_8));
        int responseCode = con.getResponseCode();
        Assert.assertEquals(expectedResponseCode, responseCode);
        if (200 == responseCode) {
            String result = IOUtils.toString(con.getInputStream(), UTF_8);
            log.debug("Result is {}", result);
            JavaType type = JsonUtil.getObjectMapper().getTypeFactory().constructCollectionType(List.class, QueryResponse.class);
            return JsonUtil.getObjectMapper().readValue(result, type);
        } else {
            throw new NotSuccessfulException();
        }
    }

    protected abstract HttpsURLConnection getUrlConnection(URL url) throws Exception;

    protected abstract HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception;

}

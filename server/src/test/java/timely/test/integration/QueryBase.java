package timely.test.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import io.netty.handler.codec.http.HttpHeaders.Names;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URL;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.request.timeseries.QueryRequest;
import timely.api.response.timeseries.QueryResponse;
import timely.util.JsonUtil;

import com.fasterxml.jackson.databind.JavaType;

public abstract class QueryBase extends MacITBase {

    public static class NotSuccessfulException extends Exception {

        private static final long serialVersionUID = 1L;
    }

    public static class UnauthorizedUserException extends Exception {

        private static final long serialVersionUID = 1L;

        public UnauthorizedUserException() {
        }
    }

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    protected void put(String... lines) throws Exception {
        StringBuffer format = new StringBuffer();
        for (String line : lines) {
            format.append("put ");
            format.append(line);
            format.append("\n");
        }
        try (Socket sock = new Socket("127.0.0.1", 54321);
                PrintWriter writer = new PrintWriter(sock.getOutputStream(), true);) {
            writer.write(format.toString());
            writer.flush();
        }
    }

    protected String query(String username, String password, String getRequest) throws Exception {
        URL url = new URL(getRequest);
        HttpsURLConnection con = getUrlConnection(username, password, url);
        int responseCode = con.getResponseCode();
        assertEquals(200, responseCode);
        String result = IOUtils.toString(con.getInputStream(), UTF_8);
        LOG.info("Result is {}", result);
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
            LOG.trace("Setting Accept header to {}", acceptType);
            con.addRequestProperty(Names.ACCEPT, acceptType);
        }
        LOG.trace("Sending HTTP Headers: {}", con.getRequestProperties());
        int responseCode = con.getResponseCode();
        assertEquals(expectedResponseCode, responseCode);
        if (200 == responseCode) {
            String result = IOUtils.toString(con.getInputStream(), UTF_8);
            LOG.info("Result is {}", result);
            return result;
        } else {
            throw new NotSuccessfulException();
        }
    }

    protected List<QueryResponse> query(String username, String password, String location, QueryRequest request)
            throws Exception {
        return query(username, password, location, request, 200);
    }

    protected List<QueryResponse> query(String username, String password, String location, QueryRequest request,
            int expectedResponseCode) throws Exception {
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
            LOG.info("Result is {}", result);
            JavaType type = JsonUtil.getObjectMapper().getTypeFactory()
                    .constructCollectionType(List.class, QueryResponse.class);
            return JsonUtil.getObjectMapper().readValue(result, type);
        } else {
            throw new NotSuccessfulException();
        }
    }

    protected List<QueryResponse> query(String location, QueryRequest request) throws Exception {
        return query(location, request, 200);
    }

    protected List<QueryResponse> query(String location, QueryRequest request, int expectedResponseCode)
            throws Exception {
        URL url = new URL(location);
        HttpsURLConnection con = getUrlConnection(url);
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        String requestJSON = JsonUtil.getObjectMapper().writeValueAsString(request);
        con.setRequestProperty("Content-Length", String.valueOf(requestJSON.length()));
        LOG.debug("Request Body JSON: {}", requestJSON);
        OutputStream wr = con.getOutputStream();
        wr.write(requestJSON.getBytes(UTF_8));
        int responseCode = con.getResponseCode();
        Assert.assertEquals(expectedResponseCode, responseCode);
        if (200 == responseCode) {
            String result = IOUtils.toString(con.getInputStream(), UTF_8);
            LOG.info("Result is {}", result);
            JavaType type = JsonUtil.getObjectMapper().getTypeFactory()
                    .constructCollectionType(List.class, QueryResponse.class);
            return JsonUtil.getObjectMapper().readValue(result, type);
        } else {
            throw new NotSuccessfulException();
        }
    }

    protected abstract HttpsURLConnection getUrlConnection(URL url) throws Exception;

    protected abstract HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception;

}

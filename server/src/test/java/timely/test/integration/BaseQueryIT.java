package timely.test.integration;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.io.OutputStream;
import java.net.URL;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.query.request.QueryRequest;
import timely.api.query.response.QueryResponse;
import timely.util.JsonUtil;

import com.fasterxml.jackson.databind.JavaType;

public abstract class BaseQueryIT {

    public static class UnauthorizedUserException extends Exception {

        private static final long serialVersionUID = 1L;

        public UnauthorizedUserException() {
        }
    }

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

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
        URL url = new URL(getRequest);
        HttpsURLConnection con = getUrlConnection(url);
        int responseCode = con.getResponseCode();
        assertEquals(200, responseCode);
        String result = IOUtils.toString(con.getInputStream(), UTF_8);
        LOG.info("Result is {}", result);
        return result;
    }

    protected List<QueryResponse> query(String username, String password, String location, QueryRequest request)
            throws Exception {
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
        String result = IOUtils.toString(con.getInputStream(), UTF_8);
        LOG.info("Result is {}", result);
        assertEquals(200, responseCode);
        JavaType type = JsonUtil.getObjectMapper().getTypeFactory()
                .constructCollectionType(List.class, QueryResponse.class);
        return JsonUtil.getObjectMapper().readValue(result, type);
    }

    protected List<QueryResponse> query(String location, QueryRequest request) throws Exception {
        URL url = new URL(location);
        HttpsURLConnection con = getUrlConnection(url);
        con.setRequestMethod("POST");
        con.setDoOutput(true);
        con.setRequestProperty("Content-Type", "application/json");
        String requestJSON = JsonUtil.getObjectMapper().writeValueAsString(request);
        con.setRequestProperty("Content-Length", String.valueOf(requestJSON.length()));
        OutputStream wr = con.getOutputStream();
        wr.write(requestJSON.getBytes(UTF_8));
        int responseCode = con.getResponseCode();
        String result = IOUtils.toString(con.getInputStream(), UTF_8);
        LOG.info("Result is {}", result);
        assertEquals(200, responseCode);
        JavaType type = JsonUtil.getObjectMapper().getTypeFactory()
                .constructCollectionType(List.class, QueryResponse.class);
        return JsonUtil.getObjectMapper().readValue(result, type);
    }

    protected abstract HttpsURLConnection getUrlConnection(URL url) throws Exception;

    protected abstract HttpsURLConnection getUrlConnection(String username, String password, URL url) throws Exception;

}

package timely.api.request;

import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;
import timely.api.annotation.Tcp;
import timely.api.annotation.WebSocket;

@Tcp(operation = "version")
@Http(path = "/api/version")
@WebSocket(operation = "version")
public class Version implements TcpRequest, HttpGetRequest, HttpPostRequest {

    public static final String VERSION = "0.0.2";

    public String getVersion() {
        return VERSION;
    }

    @Override
    public void parse(String line) {
        // do nothing
    }

    @Override
    public String toString() {
        return "Version: " + VERSION;
    }

    @Override
    public HttpPostRequest parseBody(String content) {
        return new Version();
    }

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) {
        return new Version();
    }

}

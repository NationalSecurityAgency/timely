package timely.api.request;

import io.netty.handler.codec.http.QueryStringDecoder;
import timely.api.annotation.Http;
import timely.api.annotation.Tcp;
import timely.api.annotation.WebSocket;

@Tcp(operation = "version")
@Http(path = "/version")
@WebSocket(operation = "version")
public class VersionRequest implements TcpRequest, HttpGetRequest, HttpPostRequest, WebSocketRequest {

    public static final String VERSION;
    static {
        String ver = VersionRequest.class.getPackage().getImplementationVersion();
        VERSION = (null == ver) ? "Unknown" : ver;
    }

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
        return new VersionRequest();
    }

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) {
        return new VersionRequest();
    }

}

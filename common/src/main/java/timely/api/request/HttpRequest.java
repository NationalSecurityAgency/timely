package timely.api.request;

import io.netty.handler.codec.http.FullHttpRequest;

public interface HttpRequest extends Request {

    public void setHttpRequest(FullHttpRequest httpRequest);

    public FullHttpRequest getHttpRequest();
}

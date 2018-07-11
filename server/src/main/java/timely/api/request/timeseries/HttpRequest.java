package timely.api.request.timeseries;

import io.netty.handler.codec.http.FullHttpRequest;
import timely.api.request.Request;

public interface HttpRequest extends Request {

    public void setHttpRequest(FullHttpRequest httpRequest);

    public FullHttpRequest getHttpRequest();
}

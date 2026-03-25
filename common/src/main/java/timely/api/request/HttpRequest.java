package timely.api.request;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.ReferenceCounted;

public interface HttpRequest extends Request, ReferenceCounted {

    public void setHttpRequest(FullHttpRequest httpRequest);

    public FullHttpRequest getHttpRequest();

    default int refCnt() {
        if (getHttpRequest() != null) {
            return getHttpRequest().refCnt();
        } else {
            return 1;
        }
    }

    default ReferenceCounted retain() {
        if (getHttpRequest() != null) {
            getHttpRequest().retain();
        }
        return this;
    }

    default ReferenceCounted retain(int increment) {
        if (getHttpRequest() != null) {
            getHttpRequest().retain(increment);
        }
        return this;
    }

    default ReferenceCounted touch() {
        if (getHttpRequest() != null) {
            getHttpRequest().touch();
        }
        return this;
    }

    default ReferenceCounted touch(Object hint) {
        if (getHttpRequest() != null) {
            getHttpRequest().touch(hint);
        }
        return this;
    }

    default boolean release() {
        if (getHttpRequest() != null) {
            return getHttpRequest().release();
        } else {
            return false;
        }
    }

    default boolean release(int decrement) {
        if (getHttpRequest() != null) {
            return getHttpRequest().release(decrement);
        } else {
            return false;
        }
    }
}

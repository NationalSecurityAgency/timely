package timely.server.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import timely.api.request.Request;

@ChannelHandler.Sharable
public class TestCaptureRequestHandler extends MessageToMessageDecoder<Request> {

    private static final Logger log = LoggerFactory.getLogger(TestCaptureRequestHandler.class);

    private AtomicLong counter = new AtomicLong(0);
    private List<Request> responses = new ArrayList<>();
    private boolean lastHandler;

    public TestCaptureRequestHandler(boolean lastHandler) {
        this.lastHandler = lastHandler;
    }

    public List<Request> getResponses() {
        return responses;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, Request msg, List<Object> out) throws Exception {
        log.trace("Received: {}", msg);
        responses.add(msg);
        counter.getAndIncrement();
        if (!lastHandler) {
            out.add(msg);
        }
    }

    public long getCount() {
        return counter.get();
    }

    public void clear() {
        counter.set(0);
        responses.clear();
    }
}

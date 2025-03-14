package timely.nsq;

import java.nio.charset.Charset;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.sproutsocial.nsq.MessageDataHandler;

public class TimelyNsqMessageDataHandler implements MessageDataHandler {

    private final LinkedBlockingQueue<String> messageQueue;

    public TimelyNsqMessageDataHandler(LinkedBlockingQueue<String> messageQueue) {
        this.messageQueue = messageQueue;
    }

    @Override
    public void accept(byte[] data) {
        String message = new String(data, Charset.defaultCharset());
        try {
            // use a timeout to allow for delays in the reading threads / garbage collection
            if (!messageQueue.offer(message, 5, TimeUnit.SECONDS)) {
                throw new RuntimeException(getClass().getSimpleName() + " queue full, size=" + messageQueue.size());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(getClass().getSimpleName() + " queue full, size=" + messageQueue.size());
        }
    }
}

package timely.wamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observer;
import ws.wamp.jawampa.WampClient;

/**
 *  Timely Client state observer
 */
public class StateObserver implements Observer<WampClient.State> {

    private static final Logger LOG = LoggerFactory.getLogger(StateObserver.class);

    @Override
    public void onCompleted() {
        LOG.info("WAMP state completed");
    }

    @Override
    public void onError(Throwable e) {
        LOG.error("Wamp State Error: {}", e);
    }

    @Override
    public void onNext(WampClient.State state) {
        LOG.info("WAMP state change: {}", state);
    }
}

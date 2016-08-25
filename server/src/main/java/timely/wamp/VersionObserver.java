package timely.wamp;

import rx.Observer;
import timely.api.request.VersionRequest;
import ws.wamp.jawampa.Request;

/**
 * Handles timely.version requests
 */
public class VersionObserver implements Observer<Request> {
    @Override
    public void onCompleted() {

    }

    @Override
    public void onError(Throwable e) {

    }

    @Override
    public void onNext(Request request) {
        request.reply(VersionRequest.VERSION);
    }
}

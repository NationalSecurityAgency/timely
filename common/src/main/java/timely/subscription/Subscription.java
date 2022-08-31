package timely.subscription;

import java.util.Map;

import timely.api.request.AuthenticatedRequest;
import timely.api.response.TimelyException;

public interface Subscription {

    void addMetric(AuthenticatedRequest request, String metric, Map<String,String> tags, long startTime, long endTime, long delay) throws TimelyException;

    void removeMetric(String metric);

    void scannerComplete(String metric);

    void close();

}

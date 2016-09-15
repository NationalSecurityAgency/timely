package timely.store;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;

import timely.model.Metric;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.request.timeseries.SuggestRequest;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.api.response.timeseries.SearchLookupResponse;
import timely.api.response.timeseries.SuggestResponse;

public interface DataStore {

    void store(Metric metric) throws TimelyException;

    SuggestResponse suggest(SuggestRequest query) throws TimelyException;

    SearchLookupResponse lookup(SearchLookupRequest msg) throws TimelyException;

    List<QueryResponse> query(QueryRequest msg) throws TimelyException;

    void flush() throws TimelyException;

    Scanner createScannerForMetric(String sessionId, String metric, Map<String, String> tags, long startTime, int lag)
            throws TimelyException;

}

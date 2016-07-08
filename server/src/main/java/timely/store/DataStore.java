package timely.store;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.Scanner;

import timely.api.model.Metric;
import timely.api.query.response.QueryResponse;
import timely.api.query.response.SearchLookupResponse;
import timely.api.query.response.SuggestResponse;
import timely.api.query.response.TimelyException;
import timely.api.request.QueryRequest;
import timely.api.request.SearchLookupRequest;
import timely.api.request.SuggestRequest;

public interface DataStore {

    void store(Metric metric) throws TimelyException;

    SuggestResponse suggest(SuggestRequest query) throws TimelyException;

    SearchLookupResponse lookup(SearchLookupRequest msg) throws TimelyException;

    List<QueryResponse> query(QueryRequest msg) throws TimelyException;

    void flush() throws TimelyException;

    Scanner createScannerForMetric(String sessionId, String metric, Map<String, String> tags, long startTime, int lag)
            throws TimelyException;

}

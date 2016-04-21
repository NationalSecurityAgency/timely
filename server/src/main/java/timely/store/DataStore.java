package timely.store;

import java.util.List;

import timely.api.model.Metric;
import timely.api.query.request.QueryRequest;
import timely.api.query.request.SearchLookupRequest;
import timely.api.query.request.SuggestRequest;
import timely.api.query.response.QueryResponse;
import timely.api.query.response.SearchLookupResponse;
import timely.api.query.response.SuggestResponse;
import timely.api.query.response.TimelyException;

public interface DataStore {

    void store(Metric metric) throws TimelyException;

    SuggestResponse suggest(SuggestRequest query) throws TimelyException;

    SearchLookupResponse lookup(SearchLookupRequest msg) throws TimelyException;

    List<QueryResponse> query(QueryRequest msg) throws TimelyException;

    void flush() throws TimelyException;

}

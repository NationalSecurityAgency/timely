package timely.store;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Scanner;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import timely.model.Metric;
import timely.api.request.timeseries.QueryRequest;
import timely.api.request.timeseries.SearchLookupRequest;
import timely.api.request.timeseries.SuggestRequest;
import timely.api.response.TimelyException;
import timely.api.response.timeseries.QueryResponse;
import timely.api.response.timeseries.SearchLookupResponse;
import timely.api.response.timeseries.SuggestResponse;

import timely.model.Tag;

import timely.store.cache.DataStoreCache;

public interface DataStore {

    void store(Metric metric) throws TimelyException;

    SuggestResponse suggest(SuggestRequest query) throws TimelyException;

    SearchLookupResponse lookup(SearchLookupRequest msg) throws TimelyException;

    List<QueryResponse> query(QueryRequest msg) throws TimelyException;

    void flush() throws TimelyException;

    long getAgeOffForMetric(String metricName);

    List<Range> getQueryRanges(String metric, long start, long end, Set<Tag> colFamValues);

    Set<Tag> getColumnFamilies(String metric, Map<String, String> tags) throws TableNotFoundException;

    Scanner createScannerForMetric(String sessionId, String metric, Map<String, String> tags, long startTime,
            long endTime, int lag, int scannerBatchSize, int scannerReadAhead) throws TimelyException;

    void setCache(DataStoreCache cache);

    DataStoreCache getCache();
}

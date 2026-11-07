package timely.server.store;

import java.util.Collection;
import java.util.Set;

import timely.api.request.timeseries.QueryRequest.RateOption;
import timely.api.request.timeseries.QueryRequest.SubQuery;
import timely.api.response.timeseries.QueryResponse;
import timely.model.Tag;
import timely.server.sample.Aggregation;
import timely.server.sample.Sample;

final class QueryResponseBuilder {

    private QueryResponseBuilder() {}

    static QueryResponse build(SubQuery query, Set<Tag> tags, Collection<Aggregation> values, long tsDivisor) {
        QueryResponse response = new QueryResponse();
        response.setMetric(query.getMetric());
        for (Tag tag : tags) {
            response.putTag(tag.getKey(), tag.getValue());
        }
        RateOption rateOptions = query.getRateOptions();
        Aggregation combined = Aggregation.combineAggregation(values, rateOptions);
        for (Sample entry : combined) {
            long ts = entry.getTimestamp() / tsDivisor;
            if (query.acceptsValue(entry.getValue())) {
                response.putDps(Long.toString(ts), entry.getValue());
            }
        }
        return response;
    }

}

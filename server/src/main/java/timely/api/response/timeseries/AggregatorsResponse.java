package timely.api.response.timeseries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import timely.api.response.timeseries.AggregatorsResponse.AggregatorsResponseDeserializer;
import timely.api.response.timeseries.AggregatorsResponse.AggregatorsResponseSerializer;
import timely.sample.aggregators.Avg;
import timely.sample.aggregators.Count;
import timely.sample.aggregators.Dev;
import timely.sample.aggregators.Max;
import timely.sample.aggregators.Min;
import timely.sample.aggregators.Sum;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = AggregatorsResponseSerializer.class)
@JsonDeserialize(using = AggregatorsResponseDeserializer.class)
public class AggregatorsResponse {

    public static class AggregatorsResponseSerializer extends JsonSerializer<AggregatorsResponse> {

        @Override
        public void serialize(AggregatorsResponse value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException, JsonProcessingException {
            gen.writeStartArray();
            for (String a : value.getAggregators()) {
                gen.writeString(a);
            }
            gen.writeEndArray();
        }
    }

    public static class AggregatorsResponseDeserializer extends JsonDeserializer<AggregatorsResponse> {

        @Override
        public AggregatorsResponse deserialize(JsonParser p, DeserializationContext ctxt) throws IOException,
                JsonProcessingException {
            return new AggregatorsResponse();
        }
    }

    public static final AggregatorsResponse RESPONSE = new AggregatorsResponse();
    static {
        RESPONSE.addAggregator(Avg.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Dev.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Max.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Min.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Sum.class.getSimpleName().toLowerCase());
        RESPONSE.addAggregator(Count.class.getSimpleName().toLowerCase());
    }

    private List<String> aggregators = new ArrayList<>();

    public List<String> getAggregators() {
        return aggregators;
    }

    public void setAggregators(List<String> aggregators) {
        this.aggregators = aggregators;
    }

    public void addAggregator(String agg) {
        this.aggregators.add(agg);
    }

}

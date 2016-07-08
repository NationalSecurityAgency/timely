package timely.api.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = AggregatorsResponse.class)
public class AggregatorsResponse extends JsonSerializer<AggregatorsResponse> {

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

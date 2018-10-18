package timely.api.response.timeseries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import timely.api.response.timeseries.SuggestResponse.SuggestDeserializer;
import timely.api.response.timeseries.SuggestResponse.SuggestSerializer;

@JsonSerialize(using = SuggestSerializer.class)
@JsonDeserialize(using = SuggestDeserializer.class)
public class SuggestResponse {

    public static class SuggestSerializer extends JsonSerializer<SuggestResponse> {

        @Override
        public void serialize(SuggestResponse value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException, JsonProcessingException {
            gen.writeStartArray();
            for (String s : value.getSuggestions()) {
                gen.writeString(s);
            }
            gen.writeEndArray();
        }
    }

    public static class SuggestDeserializer extends JsonDeserializer<SuggestResponse> {

        @Override
        public SuggestResponse deserialize(JsonParser p, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            SuggestResponse response = new SuggestResponse();
            if (p.getCurrentToken() == JsonToken.START_ARRAY) {
                while (p.nextToken() != JsonToken.END_ARRAY) {
                    response.addSuggestion(p.getValueAsString());
                }
            }
            return response;
        }
    }

    private List<String> suggestions = new ArrayList<>();

    public List<String> getSuggestions() {
        return suggestions;
    }

    public void setSuggestions(List<String> suggestions) {
        this.suggestions = suggestions;
    }

    public void addSuggestion(String suggestion) {
        this.suggestions.add(suggestion);
    }

}

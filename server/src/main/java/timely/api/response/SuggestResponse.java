package timely.api.response;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = SuggestResponse.class)
public class SuggestResponse extends JsonSerializer<SuggestResponse> {

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

    @Override
    public void serialize(SuggestResponse value, JsonGenerator gen, SerializerProvider serializers) throws IOException,
            JsonProcessingException {
        gen.writeStartArray();
        for (String s : value.getSuggestions()) {
            gen.writeString(s);
        }
        gen.writeEndArray();
    }

}

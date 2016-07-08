package timely.api.request;

import io.netty.handler.codec.http.QueryStringDecoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;

import timely.api.annotation.Http;
import timely.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonProperty;

@Http(path = "/api/suggest")
public class SuggestRequest extends AuthenticatedRequest implements HttpGetRequest, HttpPostRequest {

    private static List<String> validTypes = new ArrayList<>();
    static {
        validTypes.add("metrics");
        validTypes.add("tagk");
        validTypes.add("tagv");
    }

    private String type;
    @JsonProperty("q")
    private Optional<String> query = Optional.empty();
    private int max = 25;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Optional<String> getQuery() {
        return query;
    }

    public void setQuery(Optional<String> query) {
        this.query = query;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    public void validate() {
        super.validate();
        if (!validTypes.contains(this.type)) {
            throw new IllegalArgumentException("Type is not valid");
        }
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("query", query);
        tsb.append("type", type);
        tsb.append("max", max);
        return tsb.toString();
    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        return JsonUtil.getObjectMapper().readValue(content, SuggestRequest.class);
    }

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        final SuggestRequest suggest = new SuggestRequest();
        suggest.setType(decoder.parameters().get("type").get(0));
        if (decoder.parameters().containsKey("q")) {
            suggest.setQuery(Optional.of(decoder.parameters().get("q").get(0)));
        }
        if (decoder.parameters().containsKey("max")) {
            suggest.setMax(Integer.parseInt(decoder.parameters().get("max").get(0)));
        }
        return suggest;
    }

}

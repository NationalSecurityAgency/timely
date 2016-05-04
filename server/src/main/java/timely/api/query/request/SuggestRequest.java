package timely.api.query.request;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.builder.ToStringBuilder;

import timely.api.AuthenticatedRequest;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SuggestRequest extends AuthenticatedRequest {

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

}

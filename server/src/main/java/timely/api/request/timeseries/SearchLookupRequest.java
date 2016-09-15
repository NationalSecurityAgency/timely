package timely.api.request.timeseries;

import io.netty.handler.codec.http.QueryStringDecoder;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import timely.api.annotation.Http;
import timely.api.annotation.WebSocket;
import timely.model.Tag;
import timely.api.request.AuthenticatedRequest;
import timely.api.request.HttpGetRequest;
import timely.api.request.HttpPostRequest;
import timely.api.request.WebSocketRequest;
import timely.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

@Http(path = "/api/search/lookup")
@WebSocket(operation = "lookup")
public class SearchLookupRequest extends AuthenticatedRequest implements HttpGetRequest, HttpPostRequest,
        WebSocketRequest {

    @JsonProperty("metric")
    private String query;
    @JsonProperty("limit")
    private int limit = 25;
    @JsonProperty("tags")
    private Collection<Tag> tags = new ArrayList<>();

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public Collection<Tag> getTags() {
        return tags;
    }

    public void setTags(Collection<Tag> tags) {
        this.tags = tags;
    }

    public void addTag(Tag tag) {
        this.tags.add(tag);
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("query", query);
        tsb.append("limit", limit);
        tsb.append("tags", tags);
        return tsb.toString();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(query);
        hcb.append(limit);
        hcb.append(tags);
        return hcb.toHashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (null == obj) {
            return false;
        }
        if (this == obj) {
            return false;
        }
        if (obj instanceof SearchLookupRequest) {
            SearchLookupRequest other = (SearchLookupRequest) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(this.query, other.query);
            eq.append(this.limit, other.limit);
            eq.append(this.tags, other.tags);
            return eq.isEquals();
        } else {
            return false;
        }
    }

    @Override
    public HttpPostRequest parseBody(String content) throws Exception {
        // Add the operation node to the json if it does not exist for proper
        // parsing
        JsonNode root = JsonUtil.getObjectMapper().readValue(content, JsonNode.class);
        JsonNode operation = root.findValue("operation");
        if (null == operation) {
            StringBuilder buf = new StringBuilder(content.length() + 10);
            // TODO building JSON by hand? ugh
            buf.append("{ \"operation\" : \"lookup\", ");
            int open = content.indexOf("{");
            buf.append(content.substring(open + 1));
            return JsonUtil.getObjectMapper().readValue(buf.toString(), SearchLookupRequest.class);
        }
        return JsonUtil.getObjectMapper().readValue(content, SearchLookupRequest.class);
    }

    @Override
    public HttpGetRequest parseQueryParameters(QueryStringDecoder decoder) throws Exception {
        final SearchLookupRequest search = new SearchLookupRequest();
        if (!decoder.parameters().containsKey("m")) {
            throw new IllegalArgumentException("m parameter is required for lookup");
        }
        final String m = decoder.parameters().get("m").get(0);
        // TODO are you parsing json yourself here? that's always a bad idea.
        final int tagIdx = m.indexOf("{");
        if (-1 == tagIdx) {
            search.setQuery(m);
        } else {
            search.setQuery(m.substring(0, tagIdx));
            final String[] tags = m.substring(tagIdx + 1, m.length() - 1).split(",");
            for (final String tag : tags) {
                final String[] tParts = tag.split("=");
                final Tag t = new Tag(tParts[0], tParts[1]);
                search.addTag(t);
            }
        }
        if (decoder.parameters().containsKey("limit")) {
            search.setLimit(Integer.parseInt(decoder.parameters().get("limit").get(0)));
        }
        return search;
    }

}

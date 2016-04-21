package timely.api.query.request;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import timely.api.Request;
import timely.api.model.Tag;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SearchLookupRequest implements Request {

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

}

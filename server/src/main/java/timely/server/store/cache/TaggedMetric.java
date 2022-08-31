package timely.server.store.cache;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.builder.CompareToBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import timely.model.Tag;

public class TaggedMetric implements Comparable<TaggedMetric> {

    public static final ColumnVisibility EMPTY_VISIBILITY = new ColumnVisibility();
    public static final String VISIBILITY_TAG = "viz";
    private ColumnVisibility columnVisibility;
    private OrderedTags orderedTags;

    public TaggedMetric(List<Tag> tags) {
        Map<String,String> tagMap = new LinkedHashMap<>();
        for (Tag t : tags) {
            tagMap.put(t.getKey(), t.getValue());
        }
        this.columnVisibility = extractVisibility(tagMap);
        this.orderedTags = new OrderedTags(tagMap);
    }

    public Map<String,String> getTags() {
        return orderedTags.getTags();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TaggedMetric)) {
            return false;
        }
        TaggedMetric o = (TaggedMetric) obj;
        EqualsBuilder eb = new EqualsBuilder();
        eb.append(this.columnVisibility.toString(), o.columnVisibility.toString());
        eb.append(this.orderedTags, o.orderedTags);
        return eb.isEquals();
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder();
        hcb.append(columnVisibility);
        hcb.append(orderedTags);
        return hcb.toHashCode();
    }

    @Override
    public int compareTo(TaggedMetric o) {
        CompareToBuilder ctb = new CompareToBuilder();
        ctb.append(this.columnVisibility, o.columnVisibility);
        ctb.append(this.orderedTags, o.orderedTags);
        return ctb.toComparison();
    }

    public boolean matches(Map<String,String> tags) {
        return orderedTags.matches(tags);
    }

    public boolean isVisible(VisibilityFilter visibilityFilter) {

        if (columnVisibility == null) {
            return true;
        } else {
            return visibilityFilter.accept(columnVisibility);
        }
    }

    private static ColumnVisibility extractVisibility(Map<String,String> tags) {

        if (tags.containsKey(VISIBILITY_TAG)) {
            return new ColumnVisibility(tags.get(VISIBILITY_TAG));
        } else {
            return EMPTY_VISIBILITY;
        }
    }
}

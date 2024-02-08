package timely.store.cache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

public class OrderedTags {

    private Set<Pair<String,String>> tagSet = new TreeSet<>();
    private static final Pattern REGEX_TEST = Pattern.compile("^\\w+$");

    public OrderedTags(Map<String,String> tags) {
        for (Map.Entry<String,String> entry : tags.entrySet()) {
            tagSet.add(Pair.of(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public String toString() {
        return tagSet.toString();
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (null == o || getClass() != o.getClass())
            return false;
        OrderedTags other = (OrderedTags) o;
        return this.tagSet.equals(other.tagSet);
    }

    public Map<String,String> getTags() {
        Map<String,String> t = new LinkedHashMap<>();
        for (Pair<String,String> p : tagSet) {
            t.put(p.getLeft(), p.getRight());
        }
        return t;
    }

    public boolean matches(Map<String,String> tags) {
        int matches = 0;
        if (tags.isEmpty()) {
            return true;
        } else {
            Iterator<Map.Entry<String,String>> requestedTagItr = tags.entrySet().iterator();
            boolean matchFailure = false;
            while (requestedTagItr.hasNext() && !matchFailure) {
                Map.Entry<String,String> entry = requestedTagItr.next();
                Iterator<Pair<String,String>> storedTagItr = tagSet.iterator();
                boolean matchSuccess = false;
                while (storedTagItr.hasNext() && !matchSuccess && !matchFailure) {
                    Pair<String,String> p = storedTagItr.next();
                    if (entry.getKey().equals(p.getLeft())) {
                        boolean regex = isTagValueRegex(entry.getValue());
                        if (regex) {
                            if (p.getRight().matches(entry.getValue())) {
                                matches++;
                                matchSuccess = true;
                            } else {
                                matchFailure = true;
                            }
                        } else {
                            if (p.getRight().equals(entry.getValue())) {
                                matches++;
                                matchSuccess = true;
                            } else {
                                matchFailure = true;
                            }
                        }
                    }
                }
            }
            return matches == tags.size();
        }
    }

    private boolean isTagValueRegex(String value) {
        return !REGEX_TEST.matcher(value).matches();
    }
}

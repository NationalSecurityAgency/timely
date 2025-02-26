package timely.model.parse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import timely.model.Tag;

/**
 * Parses tag1=val1,tag2=val2,tag3=val3 into a {@code List} of {@code Tag} Combines {@code List} of {@code Tag} into {@code String} of
 * tag1=val1,tag2=val2,tag3=val3.
 */
public class TagListParser implements Parser<List<Tag>>, Combiner<Collection<Tag>> {

    private static final TagParser tagParser = new TagParser();

    @Override
    public List<Tag> parse(String t) {
        ArrayList<Tag> tags = new ArrayList<>();
        // only split on commas that are not preceded with a backslash
        String[] parts = t.split("(?<!\\\\),");
        for (String p : parts) {
            if (!p.isEmpty()) {
                Tag tag = tagParser.parse(p);
                // unescape commas in the key and value of the tag
                tags.add(unescapeCommas(tag));
            }
        }
        return tags;
    }

    @Override
    public String combine(Collection<Tag> tags) {
        StringBuilder builder = new StringBuilder();
        if (!tags.isEmpty()) {
            tags.forEach(p -> {
                builder.append(tagParser.combine(escapeCommas(p))).append(',');
            });
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    /**
     * An alternative combiner for a map of string to string instead of a list of tags
     *
     * @return string representation
     */
    public String combine(Map<String,String> tags) {
        return combine(tags.entrySet().stream().map(e -> new Tag(e.getKey(), e.getValue())).collect(Collectors.toList()));
    }

    private Tag escapeCommas(Tag tag) {
        return escapeCommas(tag.getKey(), tag.getValue());
    }

    private Tag escapeCommas(String k, String v) {
        k = k.replaceAll(",", "\\\\,");
        v = v.replaceAll(",", "\\\\,");
        return new Tag(k, v);
    }

    private Tag unescapeCommas(Tag tag) {
        return unescapeCommas(tag.getKey(), tag.getValue());
    }

    private Tag unescapeCommas(String k, String v) {
        k = k.replaceAll("\\\\,", ",");
        v = v.replaceAll("\\\\,", ",");
        return new Tag(k, v);
    }
}

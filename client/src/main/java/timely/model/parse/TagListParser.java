package timely.model.parse;

import com.google.common.base.Splitter;
import timely.model.Tag;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Parses tag1=val1,tag2=val2,tag3=val3 into a {@code List} of {@code Tag}
 * Combines {@code List} of {@code Tag} into {@code String} of
 * tag1=val1,tag2=val2,tag3=val3 NOTE: the tag and the value are both URL
 * encoded and subsequently URL decoded.
 */
public class TagListParser implements Parser<List<Tag>>, Combiner<Collection<Tag>> {

    private static final Splitter commaSplitter = Splitter.on(",").omitEmptyStrings();
    private static final TagParser tagParser = new TagParser();

    @Override
    public List<Tag> parse(String t) {
        ArrayList<Tag> tags = new ArrayList<>();
        commaSplitter.splitToList(t).forEach(p -> tags.add(tagParser.parse(p)));
        return tags;
    }

    @Override
    public String combine(Collection<Tag> tags) {
        StringBuilder builder = new StringBuilder();
        if (!tags.isEmpty()) {
            tags.forEach(p -> builder.append(tagParser.combine(p)).append(','));
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    /**
     * An alternative combiner for a map of string to string instead of a list
     * of tags
     * 
     * @param tags
     * @return string representation
     */
    public String combine(Map<String, String> tags) {
        StringBuilder builder = new StringBuilder();
        if (!tags.isEmpty()) {
            tags.forEach((k, v) -> builder.append(tagParser.combine(new Tag(k, v))).append(','));
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }
}
